from __future__ import annotations

import json
from dataclasses import dataclass

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from compliance_gate.Engine.errors import GuardrailViolation
from compliance_gate.Engine.models import (
    EngineSegmentDefinition,
    EngineSegmentVersion,
    EngineTransformationDefinition,
    EngineTransformationVersion,
    EngineViewDefinition,
    EngineViewVersion,
)
from compliance_gate.Engine.segments import SegmentPayloadV1
from compliance_gate.Engine.transformations import TransformationPayloadV1
from compliance_gate.Engine.views import ViewPayloadV1


@dataclass(slots=True)
class TransformationRecord:
    definition: EngineTransformationDefinition
    version: EngineTransformationVersion
    payload: TransformationPayloadV1


@dataclass(slots=True)
class SegmentRecord:
    definition: EngineSegmentDefinition
    version: EngineSegmentVersion
    payload: SegmentPayloadV1


@dataclass(slots=True)
class ViewRecord:
    definition: EngineViewDefinition
    version: EngineViewVersion
    payload: ViewPayloadV1


def list_transformations(db: Session, *, tenant_id: str) -> list[TransformationRecord]:
    definitions = (
        db.query(EngineTransformationDefinition)
        .filter(EngineTransformationDefinition.tenant_id == tenant_id)
        .order_by(EngineTransformationDefinition.created_at.desc())
        .all()
    )
    return [_load_transformation_record(db, definition) for definition in definitions]


def get_transformation(db: Session, *, tenant_id: str, transformation_id: str) -> TransformationRecord:
    definition = (
        db.query(EngineTransformationDefinition)
        .filter(
            EngineTransformationDefinition.id == transformation_id,
            EngineTransformationDefinition.tenant_id == tenant_id,
        )
        .first()
    )
    if not definition:
        raise GuardrailViolation(
            "Transformation não encontrada para o tenant.",
            details={"transformation_id": transformation_id, "reason": "transformation_not_found"},
            hint="Revise o identificador informado.",
        )
    return _load_transformation_record(db, definition)


def create_transformation(
    db: Session,
    *,
    tenant_id: str,
    name: str,
    description: str | None,
    created_by: str | None,
    payload: TransformationPayloadV1,
) -> TransformationRecord:
    definition = EngineTransformationDefinition(
        tenant_id=tenant_id,
        name=name,
        description=description,
        active_version=1,
        created_by=created_by,
    )
    db.add(definition)
    db.flush()

    version = EngineTransformationVersion(
        transformation_id=definition.id,
        version=1,
        payload_json=_dump_payload(payload),
        created_by=created_by,
    )
    db.add(version)
    _commit(db, entity="transformation", name=name)
    return TransformationRecord(definition=definition, version=version, payload=payload)


def update_transformation(
    db: Session,
    *,
    tenant_id: str,
    transformation_id: str,
    name: str | None,
    description: str | None,
    created_by: str | None,
    payload: TransformationPayloadV1,
) -> TransformationRecord:
    base = get_transformation(db, tenant_id=tenant_id, transformation_id=transformation_id)
    definition = base.definition
    definition.name = name or definition.name
    definition.description = description if description is not None else definition.description

    next_version = definition.active_version + 1
    definition.active_version = next_version

    version = EngineTransformationVersion(
        transformation_id=definition.id,
        version=next_version,
        payload_json=_dump_payload(payload),
        created_by=created_by,
    )
    db.add(version)
    _commit(db, entity="transformation", name=definition.name)
    return TransformationRecord(definition=definition, version=version, payload=payload)


def list_segments(db: Session, *, tenant_id: str) -> list[SegmentRecord]:
    definitions = (
        db.query(EngineSegmentDefinition)
        .filter(EngineSegmentDefinition.tenant_id == tenant_id)
        .order_by(EngineSegmentDefinition.created_at.desc())
        .all()
    )
    return [_load_segment_record(db, definition) for definition in definitions]


def get_segment(db: Session, *, tenant_id: str, segment_id: str) -> SegmentRecord:
    definition = (
        db.query(EngineSegmentDefinition)
        .filter(
            EngineSegmentDefinition.id == segment_id,
            EngineSegmentDefinition.tenant_id == tenant_id,
        )
        .first()
    )
    if not definition:
        raise GuardrailViolation(
            "Segment não encontrado para o tenant.",
            details={"segment_id": segment_id, "reason": "segment_not_found"},
            hint="Revise o identificador informado.",
        )
    return _load_segment_record(db, definition)


def create_segment(
    db: Session,
    *,
    tenant_id: str,
    name: str,
    description: str | None,
    created_by: str | None,
    payload: SegmentPayloadV1,
) -> SegmentRecord:
    definition = EngineSegmentDefinition(
        tenant_id=tenant_id,
        name=name,
        description=description,
        active_version=1,
        created_by=created_by,
    )
    db.add(definition)
    db.flush()

    version = EngineSegmentVersion(
        segment_id=definition.id,
        version=1,
        payload_json=_dump_payload(payload),
        created_by=created_by,
    )
    db.add(version)
    _commit(db, entity="segment", name=name)
    return SegmentRecord(definition=definition, version=version, payload=payload)


def update_segment(
    db: Session,
    *,
    tenant_id: str,
    segment_id: str,
    name: str | None,
    description: str | None,
    created_by: str | None,
    payload: SegmentPayloadV1,
) -> SegmentRecord:
    base = get_segment(db, tenant_id=tenant_id, segment_id=segment_id)
    definition = base.definition
    definition.name = name or definition.name
    definition.description = description if description is not None else definition.description

    next_version = definition.active_version + 1
    definition.active_version = next_version

    version = EngineSegmentVersion(
        segment_id=definition.id,
        version=next_version,
        payload_json=_dump_payload(payload),
        created_by=created_by,
    )
    db.add(version)
    _commit(db, entity="segment", name=definition.name)
    return SegmentRecord(definition=definition, version=version, payload=payload)


def list_views(db: Session, *, tenant_id: str) -> list[ViewRecord]:
    definitions = (
        db.query(EngineViewDefinition)
        .filter(EngineViewDefinition.tenant_id == tenant_id)
        .order_by(EngineViewDefinition.created_at.desc())
        .all()
    )
    return [_load_view_record(db, definition) for definition in definitions]


def get_view(db: Session, *, tenant_id: str, view_id: str) -> ViewRecord:
    definition = (
        db.query(EngineViewDefinition)
        .filter(
            EngineViewDefinition.id == view_id,
            EngineViewDefinition.tenant_id == tenant_id,
        )
        .first()
    )
    if not definition:
        raise GuardrailViolation(
            "View não encontrada para o tenant.",
            details={"view_id": view_id, "reason": "view_not_found"},
            hint="Revise o identificador informado.",
        )
    return _load_view_record(db, definition)


def create_view(
    db: Session,
    *,
    tenant_id: str,
    name: str,
    description: str | None,
    created_by: str | None,
    payload: ViewPayloadV1,
) -> ViewRecord:
    definition = EngineViewDefinition(
        tenant_id=tenant_id,
        name=name,
        description=description,
        active_version=1,
        created_by=created_by,
    )
    db.add(definition)
    db.flush()

    version = EngineViewVersion(
        view_id=definition.id,
        version=1,
        payload_json=_dump_payload(payload),
        created_by=created_by,
    )
    db.add(version)
    _commit(db, entity="view", name=name)
    return ViewRecord(definition=definition, version=version, payload=payload)


def update_view(
    db: Session,
    *,
    tenant_id: str,
    view_id: str,
    name: str | None,
    description: str | None,
    created_by: str | None,
    payload: ViewPayloadV1,
) -> ViewRecord:
    base = get_view(db, tenant_id=tenant_id, view_id=view_id)
    definition = base.definition
    definition.name = name or definition.name
    definition.description = description if description is not None else definition.description

    next_version = definition.active_version + 1
    definition.active_version = next_version

    version = EngineViewVersion(
        view_id=definition.id,
        version=next_version,
        payload_json=_dump_payload(payload),
        created_by=created_by,
    )
    db.add(version)
    _commit(db, entity="view", name=definition.name)
    return ViewRecord(definition=definition, version=version, payload=payload)


def _load_transformation_record(
    db: Session,
    definition: EngineTransformationDefinition,
) -> TransformationRecord:
    version = (
        db.query(EngineTransformationVersion)
        .filter(
            EngineTransformationVersion.transformation_id == definition.id,
            EngineTransformationVersion.version == definition.active_version,
        )
        .first()
    )
    if not version:
        raise GuardrailViolation(
            "Versão ativa de transformation não encontrada.",
            details={"transformation_id": definition.id, "reason": "active_version_not_found"},
            hint="Republique a transformation para restaurar consistência.",
        )
    payload = TransformationPayloadV1.model_validate(_load_payload(version.payload_json))
    return TransformationRecord(definition=definition, version=version, payload=payload)


def _load_segment_record(db: Session, definition: EngineSegmentDefinition) -> SegmentRecord:
    version = (
        db.query(EngineSegmentVersion)
        .filter(
            EngineSegmentVersion.segment_id == definition.id,
            EngineSegmentVersion.version == definition.active_version,
        )
        .first()
    )
    if not version:
        raise GuardrailViolation(
            "Versão ativa de segment não encontrada.",
            details={"segment_id": definition.id, "reason": "active_version_not_found"},
            hint="Republique o segment para restaurar consistência.",
        )
    payload = SegmentPayloadV1.model_validate(_load_payload(version.payload_json))
    return SegmentRecord(definition=definition, version=version, payload=payload)


def _load_view_record(db: Session, definition: EngineViewDefinition) -> ViewRecord:
    version = (
        db.query(EngineViewVersion)
        .filter(
            EngineViewVersion.view_id == definition.id,
            EngineViewVersion.version == definition.active_version,
        )
        .first()
    )
    if not version:
        raise GuardrailViolation(
            "Versão ativa de view não encontrada.",
            details={"view_id": definition.id, "reason": "active_version_not_found"},
            hint="Republique a view para restaurar consistência.",
        )
    payload = ViewPayloadV1.model_validate(_load_payload(version.payload_json))
    return ViewRecord(definition=definition, version=version, payload=payload)


def _dump_payload(payload: TransformationPayloadV1 | SegmentPayloadV1 | ViewPayloadV1) -> str:
    return payload.model_dump_json()


def _load_payload(payload_json: str) -> dict:
    return json.loads(payload_json)


def _commit(db: Session, *, entity: str, name: str) -> None:
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise GuardrailViolation(
            f"Já existe {entity} com este nome para o tenant.",
            details={"entity": entity, "name": name, "reason": "unique_violation"},
            hint="Use um nome diferente para evitar conflito.",
        ) from exc

