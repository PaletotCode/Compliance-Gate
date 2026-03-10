from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from compliance_gate.Engine.errors import DeclarativeEngineError, GuardrailViolation
from compliance_gate.Engine.expressions import ExpressionDataType, ExpressionValidationOptions
from compliance_gate.Engine.models import (
    EngineRuleBlock,
    EngineRuleEntry,
    EngineRuleSetDefinition,
    EngineRuleSetVersion,
)
from compliance_gate.Engine.rulesets.schemas import (
    RuleSetPayloadV2,
    RuleSetValidationIssue,
    RuleSetValidationResult,
    RuleSetVersionStatus,
)
from compliance_gate.infra.db.models import AuditLog


@dataclass(slots=True)
class RuleSetRecord:
    definition: EngineRuleSetDefinition
    version: EngineRuleSetVersion
    payload: RuleSetPayloadV2


@dataclass(slots=True)
class RuleSetVersionRecord:
    definition: EngineRuleSetDefinition
    version: EngineRuleSetVersion
    payload: RuleSetPayloadV2


def list_rulesets(
    db: Session,
    *,
    tenant_id: str,
    include_archived: bool = False,
) -> list[RuleSetRecord]:
    query = db.query(EngineRuleSetDefinition).filter(EngineRuleSetDefinition.tenant_id == tenant_id)
    if not include_archived:
        query = query.filter(EngineRuleSetDefinition.is_archived.is_(False))

    definitions = query.order_by(EngineRuleSetDefinition.created_at.desc()).all()
    return [
        _load_ruleset_record(db, definition=definition, version_number=None)
        for definition in definitions
    ]


def get_ruleset(
    db: Session,
    *,
    tenant_id: str,
    ruleset_id: str,
    version: int | None = None,
    include_archived: bool = False,
) -> RuleSetRecord:
    definition = _get_ruleset_definition(
        db,
        tenant_id=tenant_id,
        ruleset_id=ruleset_id,
        include_archived=include_archived,
    )
    return _load_ruleset_record(db, definition=definition, version_number=version)


def get_ruleset_by_name(
    db: Session,
    *,
    tenant_id: str,
    name: str,
    resolution: Literal["active", "published"] = "active",
) -> RuleSetRecord:
    definition = (
        db.query(EngineRuleSetDefinition)
        .filter(
            EngineRuleSetDefinition.tenant_id == tenant_id,
            EngineRuleSetDefinition.name == name,
            EngineRuleSetDefinition.is_archived.is_(False),
        )
        .first()
    )
    if not definition:
        raise GuardrailViolation(
            "RuleSet não encontrado para o tenant.",
            details={"ruleset_name": name, "reason": "ruleset_not_found"},
            hint="Revise o identificador informado.",
        )

    if resolution == "published":
        version = definition.published_version
        if version is None:
            raise GuardrailViolation(
                "RuleSet não possui versão publicada.",
                details={"ruleset_id": definition.id, "reason": "no_published_version"},
                hint="Valide e publique uma versão antes de consumir o ruleset.",
            )
        return _load_ruleset_record(db, definition=definition, version_number=version)

    return _load_ruleset_record(db, definition=definition, version_number=definition.active_version)


def create_ruleset(
    db: Session,
    *,
    tenant_id: str,
    name: str,
    description: str | None,
    created_by: str | None,
    payload: RuleSetPayloadV2,
) -> RuleSetRecord:
    definition = EngineRuleSetDefinition(
        tenant_id=tenant_id,
        name=name,
        description=description,
        active_version=1,
        published_version=None,
        is_archived=False,
        created_by=created_by,
    )
    db.add(definition)
    db.flush()

    version = EngineRuleSetVersion(
        ruleset_id=definition.id,
        tenant_id=tenant_id,
        version=1,
        status=RuleSetVersionStatus.DRAFT.value,
        payload_json=_dump_payload(payload),
        created_by=created_by,
    )
    db.add(version)
    db.flush()

    _sync_version_blocks(
        db,
        version_id=version.id,
        payload=payload,
        actor=created_by,
    )
    _audit_ruleset_action(
        db,
        tenant_id=tenant_id,
        actor=created_by,
        action="RULESET_CREATE",
        ruleset_id=definition.id,
        version=1,
        details={"name": name},
    )
    _commit(db, entity="ruleset", name=name)
    return _load_ruleset_record(db, definition=definition, version_number=1)


def update_ruleset(
    db: Session,
    *,
    tenant_id: str,
    ruleset_id: str,
    name: str | None,
    description: str | None,
    updated_by: str | None,
) -> RuleSetRecord:
    definition = _get_ruleset_definition(
        db,
        tenant_id=tenant_id,
        ruleset_id=ruleset_id,
        include_archived=False,
    )
    if name:
        definition.name = name
    if description is not None:
        definition.description = description

    _audit_ruleset_action(
        db,
        tenant_id=tenant_id,
        actor=updated_by,
        action="RULESET_UPDATE",
        ruleset_id=definition.id,
        version=definition.active_version,
        details={"operation": "metadata_update"},
    )
    _commit(db, entity="ruleset", name=definition.name)
    return _load_ruleset_record(db, definition=definition, version_number=None)


def archive_ruleset(
    db: Session,
    *,
    tenant_id: str,
    ruleset_id: str,
    archived_by: str | None,
) -> RuleSetRecord:
    definition = _get_ruleset_definition(
        db,
        tenant_id=tenant_id,
        ruleset_id=ruleset_id,
        include_archived=True,
    )
    definition.is_archived = True

    versions = (
        db.query(EngineRuleSetVersion)
        .filter(EngineRuleSetVersion.ruleset_id == definition.id)
        .all()
    )
    for version in versions:
        version.status = RuleSetVersionStatus.ARCHIVED.value

    _audit_ruleset_action(
        db,
        tenant_id=tenant_id,
        actor=archived_by,
        action="RULESET_UPDATE",
        ruleset_id=definition.id,
        version=definition.active_version,
        details={"operation": "archive"},
    )
    _commit(db)
    return _load_ruleset_record(db, definition=definition, version_number=definition.active_version)


def list_ruleset_versions(
    db: Session,
    *,
    tenant_id: str,
    ruleset_id: str,
    include_archived: bool = True,
) -> list[RuleSetVersionRecord]:
    definition = _get_ruleset_definition(
        db,
        tenant_id=tenant_id,
        ruleset_id=ruleset_id,
        include_archived=include_archived,
    )
    versions_query = db.query(EngineRuleSetVersion).filter(
        EngineRuleSetVersion.ruleset_id == definition.id
    )
    if not include_archived:
        versions_query = versions_query.filter(
            EngineRuleSetVersion.status != RuleSetVersionStatus.ARCHIVED.value
        )

    versions = versions_query.order_by(EngineRuleSetVersion.version.desc()).all()
    return [
        _load_ruleset_version_record(db, definition=definition, version_number=version.version)
        for version in versions
    ]


def get_ruleset_version(
    db: Session,
    *,
    tenant_id: str,
    ruleset_id: str,
    version: int,
    include_archived: bool = True,
) -> RuleSetVersionRecord:
    definition = _get_ruleset_definition(
        db,
        tenant_id=tenant_id,
        ruleset_id=ruleset_id,
        include_archived=include_archived,
    )
    return _load_ruleset_version_record(db, definition=definition, version_number=version)


def create_ruleset_version(
    db: Session,
    *,
    tenant_id: str,
    ruleset_id: str,
    created_by: str | None,
    source_version: int | None = None,
    payload: RuleSetPayloadV2 | None = None,
) -> RuleSetVersionRecord:
    definition = _get_ruleset_definition(
        db,
        tenant_id=tenant_id,
        ruleset_id=ruleset_id,
        include_archived=False,
    )

    source = _load_ruleset_version_record(
        db,
        definition=definition,
        version_number=source_version or definition.active_version,
    )
    resolved_payload = payload or source.payload

    latest_version = (
        db.query(func.max(EngineRuleSetVersion.version))
        .filter(EngineRuleSetVersion.ruleset_id == definition.id)
        .scalar()
    )
    next_version = int(latest_version or 0) + 1

    version = EngineRuleSetVersion(
        ruleset_id=definition.id,
        tenant_id=tenant_id,
        version=next_version,
        status=RuleSetVersionStatus.DRAFT.value,
        payload_json=_dump_payload(resolved_payload),
        validation_errors_json=None,
        created_by=created_by,
    )
    db.add(version)
    db.flush()

    definition.active_version = next_version

    _sync_version_blocks(db, version_id=version.id, payload=resolved_payload, actor=created_by)
    _audit_ruleset_action(
        db,
        tenant_id=tenant_id,
        actor=created_by,
        action="RULESET_UPDATE",
        ruleset_id=definition.id,
        version=next_version,
        details={
            "operation": "new_version",
            "source_version": source.version.version,
        },
    )
    _commit(db)
    return _load_ruleset_version_record(db, definition=definition, version_number=next_version)


def update_ruleset_version(
    db: Session,
    *,
    tenant_id: str,
    ruleset_id: str,
    version: int,
    payload: RuleSetPayloadV2,
    updated_by: str | None,
) -> RuleSetVersionRecord:
    definition = _get_ruleset_definition(
        db,
        tenant_id=tenant_id,
        ruleset_id=ruleset_id,
        include_archived=False,
    )
    version_model = _get_ruleset_version_model(db, definition=definition, version_number=version)

    if version_model.status == RuleSetVersionStatus.PUBLISHED.value:
        raise GuardrailViolation(
            "Versão publicada não pode ser alterada.",
            details={
                "ruleset_id": definition.id,
                "version": version,
                "reason": "published_version_locked",
            },
            hint="Crie uma nova versão draft para editar regras publicadas.",
        )

    version_model.payload_json = _dump_payload(payload)
    version_model.status = RuleSetVersionStatus.DRAFT.value
    version_model.validation_errors_json = None
    version_model.validated_at = None
    version_model.validated_by = None

    definition.active_version = version

    _sync_version_blocks(db, version_id=version_model.id, payload=payload, actor=updated_by)
    _audit_ruleset_action(
        db,
        tenant_id=tenant_id,
        actor=updated_by,
        action="RULESET_UPDATE",
        ruleset_id=definition.id,
        version=version,
        details={"operation": "edit_version"},
    )
    _commit(db)
    return _load_ruleset_version_record(db, definition=definition, version_number=version)


def validate_ruleset_version(
    db: Session,
    *,
    tenant_id: str,
    ruleset_id: str,
    version: int,
    column_types: dict[str, str | ExpressionDataType],
    validated_by: str | None,
    options: ExpressionValidationOptions | None = None,
) -> tuple[RuleSetVersionRecord, RuleSetValidationResult]:
    definition = _get_ruleset_definition(
        db,
        tenant_id=tenant_id,
        ruleset_id=ruleset_id,
        include_archived=False,
    )
    version_model = _get_ruleset_version_model(db, definition=definition, version_number=version)
    payload = _load_payload(version_model.payload_json)

    try:
        payload.validate_types(column_types=column_types, options=options)
    except DeclarativeEngineError as exc:
        issue = _validation_issue_from_exception(exc)
        version_model.status = RuleSetVersionStatus.DRAFT.value
        version_model.validation_errors_json = json.dumps([issue.model_dump()], ensure_ascii=False)
        version_model.validated_at = datetime.now(UTC)
        version_model.validated_by = validated_by
        _audit_ruleset_action(
            db,
            tenant_id=tenant_id,
            actor=validated_by,
            action="RULESET_VALIDATE",
            ruleset_id=definition.id,
            version=version,
            details={"is_valid": False, "issues": [issue.model_dump()]},
        )
        _commit(db)
        raise

    version_model.status = RuleSetVersionStatus.VALIDATED.value
    version_model.validation_errors_json = json.dumps([], ensure_ascii=False)
    version_model.validated_at = datetime.now(UTC)
    version_model.validated_by = validated_by

    _audit_ruleset_action(
        db,
        tenant_id=tenant_id,
        actor=validated_by,
        action="RULESET_VALIDATE",
        ruleset_id=definition.id,
        version=version,
        details={"is_valid": True},
    )
    _commit(db)

    return (
        _load_ruleset_version_record(db, definition=definition, version_number=version),
        RuleSetValidationResult(is_valid=True, issues=[]),
    )


def publish_ruleset_version(
    db: Session,
    *,
    tenant_id: str,
    ruleset_id: str,
    version: int,
    published_by: str | None,
) -> RuleSetVersionRecord:
    definition = _get_ruleset_definition(
        db,
        tenant_id=tenant_id,
        ruleset_id=ruleset_id,
        include_archived=False,
    )
    target = _get_ruleset_version_model(db, definition=definition, version_number=version)

    if target.status not in {
        RuleSetVersionStatus.VALIDATED.value,
        RuleSetVersionStatus.PUBLISHED.value,
    }:
        raise GuardrailViolation(
            "Somente versões validadas podem ser publicadas.",
            details={
                "ruleset_id": definition.id,
                "version": version,
                "status": target.status,
                "reason": "version_not_validated",
            },
            hint="Valide a versão antes da publicação.",
        )

    previous_published = definition.published_version
    if previous_published and previous_published != version:
        previous = _get_ruleset_version_model(
            db,
            definition=definition,
            version_number=previous_published,
        )
        previous.status = RuleSetVersionStatus.ARCHIVED.value

    now = datetime.now(UTC)
    target.status = RuleSetVersionStatus.PUBLISHED.value
    target.published_at = now
    target.published_by = published_by
    if target.validated_at is None:
        target.validated_at = now
        target.validated_by = published_by

    definition.published_version = version
    definition.active_version = version

    _audit_ruleset_action(
        db,
        tenant_id=tenant_id,
        actor=published_by,
        action="RULESET_PUBLISH",
        ruleset_id=definition.id,
        version=version,
        details={"previous_published_version": previous_published},
    )
    _commit(db)

    return _load_ruleset_version_record(db, definition=definition, version_number=version)


def rollback_ruleset(
    db: Session,
    *,
    tenant_id: str,
    ruleset_id: str,
    rolled_back_by: str | None,
    target_version: int | None = None,
) -> RuleSetVersionRecord:
    definition = _get_ruleset_definition(
        db,
        tenant_id=tenant_id,
        ruleset_id=ruleset_id,
        include_archived=False,
    )

    current_published = definition.published_version
    if current_published is None:
        raise GuardrailViolation(
            "Não há versão publicada para rollback.",
            details={"ruleset_id": definition.id, "reason": "no_published_version"},
            hint="Publique uma versão antes de executar rollback.",
        )

    resolved_target_version = target_version
    if resolved_target_version is None:
        previous = (
            db.query(EngineRuleSetVersion)
            .filter(
                EngineRuleSetVersion.ruleset_id == definition.id,
                EngineRuleSetVersion.version < current_published,
            )
            .order_by(EngineRuleSetVersion.version.desc())
            .first()
        )
        if previous is None:
            raise GuardrailViolation(
                "Não existe versão anterior para rollback.",
                details={"ruleset_id": definition.id, "reason": "no_previous_version"},
                hint="Informe target_version explicitamente ou publique novas versões.",
            )
        resolved_target_version = previous.version

    target = _get_ruleset_version_model(
        db,
        definition=definition,
        version_number=resolved_target_version,
    )

    if current_published != resolved_target_version:
        current = _get_ruleset_version_model(
            db,
            definition=definition,
            version_number=current_published,
        )
        current.status = RuleSetVersionStatus.ARCHIVED.value

    target.status = RuleSetVersionStatus.PUBLISHED.value
    target.published_at = datetime.now(UTC)
    target.published_by = rolled_back_by

    definition.published_version = resolved_target_version
    definition.active_version = resolved_target_version

    _audit_ruleset_action(
        db,
        tenant_id=tenant_id,
        actor=rolled_back_by,
        action="RULESET_ROLLBACK",
        ruleset_id=definition.id,
        version=resolved_target_version,
        details={
            "from_version": current_published,
            "to_version": resolved_target_version,
        },
    )
    _commit(db)

    return _load_ruleset_version_record(
        db,
        definition=definition,
        version_number=resolved_target_version,
    )


def _load_ruleset_record(
    db: Session,
    *,
    definition: EngineRuleSetDefinition,
    version_number: int | None,
) -> RuleSetRecord:
    version = _get_ruleset_version_model(
        db,
        definition=definition,
        version_number=version_number or definition.active_version,
    )
    payload = _load_payload(version.payload_json)
    return RuleSetRecord(definition=definition, version=version, payload=payload)


def _load_ruleset_version_record(
    db: Session,
    *,
    definition: EngineRuleSetDefinition,
    version_number: int,
) -> RuleSetVersionRecord:
    version = _get_ruleset_version_model(db, definition=definition, version_number=version_number)
    payload = _load_payload(version.payload_json)
    return RuleSetVersionRecord(definition=definition, version=version, payload=payload)


def _get_ruleset_definition(
    db: Session,
    *,
    tenant_id: str,
    ruleset_id: str,
    include_archived: bool,
) -> EngineRuleSetDefinition:
    definition = (
        db.query(EngineRuleSetDefinition)
        .filter(
            EngineRuleSetDefinition.id == ruleset_id,
            EngineRuleSetDefinition.tenant_id == tenant_id,
        )
        .first()
    )
    if not definition:
        raise GuardrailViolation(
            "RuleSet não encontrado para o tenant.",
            details={"ruleset_id": ruleset_id, "reason": "ruleset_not_found"},
            hint="Revise o identificador informado.",
        )
    if definition.is_archived and not include_archived:
        raise GuardrailViolation(
            "RuleSet arquivado.",
            details={"ruleset_id": ruleset_id, "reason": "ruleset_archived"},
            hint="Restaure ou consulte com include_archived=true.",
        )
    return definition


def _get_ruleset_version_model(
    db: Session,
    *,
    definition: EngineRuleSetDefinition,
    version_number: int,
) -> EngineRuleSetVersion:
    version = (
        db.query(EngineRuleSetVersion)
        .filter(
            EngineRuleSetVersion.ruleset_id == definition.id,
            EngineRuleSetVersion.version == version_number,
        )
        .first()
    )
    if not version:
        raise GuardrailViolation(
            "Versão do RuleSet não encontrada.",
            details={
                "ruleset_id": definition.id,
                "version": version_number,
                "reason": "ruleset_version_not_found",
            },
            hint="Revise o número da versão informado.",
        )
    return version


def _sync_version_blocks(
    db: Session,
    *,
    version_id: str,
    payload: RuleSetPayloadV2,
    actor: str | None,
) -> None:
    old_blocks = (
        db.query(EngineRuleBlock).filter(EngineRuleBlock.ruleset_version_id == version_id).all()
    )
    for block in old_blocks:
        db.delete(block)
    db.flush()

    for block_index, block in enumerate(payload.blocks):
        block_model = EngineRuleBlock(
            ruleset_version_id=version_id,
            block_type=block.kind.value,
            execution_mode=block.execution_mode.value,
            order_index=block_index,
        )
        db.add(block_model)
        db.flush()

        ordered_entries = sorted(
            enumerate(block.entries),
            key=lambda item: (item[1].priority, item[0]),
        )
        for entry_index, (_, entry) in enumerate(ordered_entries):
            db.add(
                EngineRuleEntry(
                    rule_block_id=block_model.id,
                    rule_key=entry.rule_key,
                    priority=entry.priority,
                    condition_json=json.dumps(entry.condition.model_dump(), ensure_ascii=False),
                    output_json=json.dumps(entry.output, ensure_ascii=False, default=str),
                    description=entry.description,
                    order_index=entry_index,
                    created_by=actor,
                )
            )


def _audit_ruleset_action(
    db: Session,
    *,
    tenant_id: str,
    actor: str | None,
    action: str,
    ruleset_id: str,
    version: int | None,
    details: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = dict(details or {})
    payload["version"] = version
    db.add(
        AuditLog(
            tenant_id=tenant_id,
            dataset_version_id=None,
            actor=actor or "system",
            action=action,
            entity_type="engine_ruleset",
            entity_id=ruleset_id,
            details=json.dumps(payload, ensure_ascii=False, default=str),
        )
    )


def _dump_payload(payload: RuleSetPayloadV2) -> str:
    return payload.model_dump_json()


def _load_payload(payload_json: str) -> RuleSetPayloadV2:
    return RuleSetPayloadV2.model_validate(json.loads(payload_json))


def _validation_issue_from_exception(exc: DeclarativeEngineError) -> RuleSetValidationIssue:
    payload = exc.to_payload()
    return RuleSetValidationIssue(
        code=payload.code,
        message=payload.message,
        details=payload.details,
        hint=payload.hint,
        node_path=payload.node_path,
    )


def _commit(db: Session, *, entity: str | None = None, name: str | None = None) -> None:
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        if entity and name:
            raise GuardrailViolation(
                f"Já existe {entity} com este nome para o tenant.",
                details={"entity": entity, "name": name, "reason": "unique_violation"},
                hint="Use um nome diferente para evitar conflito.",
            ) from exc
        raise GuardrailViolation(
            "Falha de integridade ao persistir RuleSet.",
            details={"reason": "integrity_error"},
            hint="Revise os dados enviados e tente novamente.",
        ) from exc
