from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from compliance_gate.Engine.config.engine_settings import engine_settings
from compliance_gate.Engine.errors import GuardrailViolation
from compliance_gate.Engine.models import (
    EngineClassificationDivergence,
    EngineClassificationMode,
    EngineRun,
)
from compliance_gate.Engine.rulesets.schemas import ClassificationRuntimeMode
from compliance_gate.infra.db.models import AuditLog


@dataclass(slots=True)
class ClassificationModeState:
    mode: ClassificationRuntimeMode
    ruleset_name: str | None
    updated_at: datetime | None
    updated_by: str | None
    is_default: bool


def get_classification_mode(
    db: Session,
    *,
    tenant_id: str,
) -> tuple[ClassificationRuntimeMode, str | None]:
    state = get_classification_mode_state(db, tenant_id=tenant_id)
    return state.mode, state.ruleset_name


def get_classification_mode_state(
    db: Session,
    *,
    tenant_id: str,
) -> ClassificationModeState:
    row = (
        db.query(EngineClassificationMode)
        .filter(EngineClassificationMode.tenant_id == tenant_id)
        .first()
    )
    if row is None:
        return ClassificationModeState(
            mode=_default_runtime_mode(),
            ruleset_name=engine_settings.classification_default_ruleset_name,
            updated_at=None,
            updated_by=None,
            is_default=True,
        )

    return ClassificationModeState(
        mode=ClassificationRuntimeMode(row.mode),
        ruleset_name=row.ruleset_name,
        updated_at=row.updated_at,
        updated_by=row.updated_by,
        is_default=False,
    )


def set_classification_mode(
    db: Session,
    *,
    tenant_id: str,
    mode: ClassificationRuntimeMode,
    ruleset_name: str | None,
    updated_by: str | None,
) -> EngineClassificationMode:
    if mode in {ClassificationRuntimeMode.SHADOW, ClassificationRuntimeMode.DECLARATIVE}:
        resolved_ruleset_name = ruleset_name or engine_settings.classification_default_ruleset_name
    else:
        resolved_ruleset_name = ruleset_name

    row = (
        db.query(EngineClassificationMode)
        .filter(EngineClassificationMode.tenant_id == tenant_id)
        .first()
    )
    if row is None:
        row = EngineClassificationMode(
            tenant_id=tenant_id,
            mode=mode.value,
            ruleset_name=resolved_ruleset_name,
            updated_by=updated_by,
        )
        db.add(row)
    else:
        row.mode = mode.value
        row.ruleset_name = resolved_ruleset_name
        row.updated_by = updated_by

    _audit(
        db,
        tenant_id=tenant_id,
        actor=updated_by,
        action="CLASSIFICATION_MODE_UPDATE",
        entity_id=tenant_id,
        details={
            "mode": mode.value,
            "ruleset_name": resolved_ruleset_name,
        },
    )
    _commit(db)
    db.refresh(row)
    return row


def record_divergences(
    db: Session,
    *,
    tenant_id: str,
    dataset_version_id: str,
    run_id: str,
    ruleset_name: str,
    divergences: Sequence[dict[str, Any]],
) -> int:
    for item in divergences:
        db.add(
            EngineClassificationDivergence(
                tenant_id=tenant_id,
                dataset_version_id=dataset_version_id,
                run_id=run_id,
                ruleset_name=ruleset_name,
                machine_id=item.get("machine_id"),
                hostname=item.get("hostname"),
                legacy_primary_status=item.get("legacy_primary_status"),
                legacy_primary_status_label=item.get("legacy_primary_status_label"),
                legacy_flags_json=json.dumps(item.get("legacy_flags") or [], ensure_ascii=False),
                declarative_primary_status=item.get("declarative_primary_status"),
                declarative_primary_status_label=item.get("declarative_primary_status_label"),
                declarative_flags_json=json.dumps(
                    item.get("declarative_flags") or [], ensure_ascii=False
                ),
                diff_json=json.dumps(item.get("diff") or {}, ensure_ascii=False),
            )
        )
    return len(divergences)


def list_recent_divergences(
    db: Session,
    *,
    tenant_id: str,
    limit: int,
    dataset_version_id: str | None = None,
) -> list[EngineClassificationDivergence]:
    query = db.query(EngineClassificationDivergence).filter(
        EngineClassificationDivergence.tenant_id == tenant_id
    )
    if dataset_version_id:
        query = query.filter(
            EngineClassificationDivergence.dataset_version_id == dataset_version_id
        )
    return query.order_by(EngineClassificationDivergence.created_at.desc()).limit(limit).all()


def list_recent_classification_runs(
    db: Session,
    *,
    tenant_id: str,
    limit: int,
) -> list[EngineRun]:
    return (
        db.query(EngineRun)
        .filter(
            EngineRun.tenant_id == tenant_id,
            EngineRun.run_type == "materialize",
        )
        .order_by(EngineRun.started_at.desc())
        .limit(limit)
        .all()
    )


def _default_runtime_mode() -> ClassificationRuntimeMode:
    configured = (engine_settings.classification_mode_default or "legacy").strip().lower()
    for mode in ClassificationRuntimeMode:
        if mode.value == configured:
            return mode
    return ClassificationRuntimeMode.LEGACY


def _audit(
    db: Session,
    *,
    tenant_id: str,
    actor: str | None,
    action: str,
    entity_id: str,
    details: dict[str, Any] | None = None,
) -> None:
    db.add(
        AuditLog(
            tenant_id=tenant_id,
            dataset_version_id=None,
            actor=actor or "system",
            action=action,
            entity_type="engine_classification",
            entity_id=entity_id,
            details=json.dumps(details or {}, ensure_ascii=False, default=str),
        )
    )


def _commit(db: Session) -> None:
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise GuardrailViolation(
            "Falha ao persistir modo de classificação.",
            details={"reason": "integrity_error"},
            hint="Revise os dados enviados e tente novamente.",
        ) from exc
