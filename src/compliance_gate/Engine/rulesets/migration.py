from __future__ import annotations

import json
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from compliance_gate.authentication.models import Tenant
from compliance_gate.Engine.config.engine_settings import engine_settings
from compliance_gate.Engine.errors import GuardrailViolation
from compliance_gate.Engine.models import (
    EngineClassificationDivergence,
    EngineClassificationMigration,
    EngineRun,
    EngineRuleSetDefinition,
)
from compliance_gate.Engine.rulesets.runtime import machine_record_column_types
from compliance_gate.Engine.rulesets.schemas import (
    ClassificationMigrationPhase,
    RuleSetVersionStatus,
)
from compliance_gate.Engine.rulesets.store import (
    create_ruleset,
    create_ruleset_version,
    get_ruleset_version,
    publish_ruleset_version,
    validate_ruleset_version,
)
from compliance_gate.Engine.rulesets.template_library import (
    build_legacy_baseline_ruleset_payload,
    list_legacy_rule_inventory,
    status_severity_by_key,
)
from compliance_gate.infra.db.models import AuditLog


@dataclass(slots=True)
class ClassificationMigrationState:
    tenant_id: str
    phase: ClassificationMigrationPhase
    ruleset_id: str | None
    ruleset_name: str | None
    baseline_version: int | None
    parity_target_percent: float
    last_parity_percent: float | None
    last_parity_passed: bool | None
    last_dataset_version_id: str | None
    last_run_id: str | None
    updated_at: datetime | None
    updated_by: str | None
    is_default: bool


def ensure_baseline_ruleset_for_tenant(
    db: Session,
    *,
    tenant_id: str,
    actor: str | None,
    ruleset_name: str | None = None,
    stale_days: int | None = None,
    legacy_os_definitions: Sequence[str] | None = None,
) -> dict[str, Any]:
    resolved_ruleset_name = _resolve_ruleset_name(ruleset_name)
    resolved_stale_days = int(stale_days or engine_settings.classification_stale_days)
    resolved_legacy_terms = _resolve_legacy_os_terms(legacy_os_definitions)
    baseline_payload = build_legacy_baseline_ruleset_payload(
        stale_days=resolved_stale_days,
        legacy_os_definitions=resolved_legacy_terms,
    )
    column_types = machine_record_column_types()
    created_now = False

    definition = (
        db.query(EngineRuleSetDefinition)
        .filter(
            EngineRuleSetDefinition.tenant_id == tenant_id,
            EngineRuleSetDefinition.name == resolved_ruleset_name,
            EngineRuleSetDefinition.is_archived.is_(False),
        )
        .first()
    )

    if definition is None:
        created = create_ruleset(
            db,
            tenant_id=tenant_id,
            name=resolved_ruleset_name,
            description="Baseline oficial migrado de rule.py (legado).",
            created_by=actor,
            payload=baseline_payload,
        )
        created_now = True
        definition = created.definition
        candidate_version = created.version.version
    else:
        candidate_version = definition.active_version
        if definition.published_version is None:
            new_version = create_ruleset_version(
                db,
                tenant_id=tenant_id,
                ruleset_id=definition.id,
                created_by=actor,
                source_version=definition.active_version,
                payload=baseline_payload,
            )
            candidate_version = new_version.version.version
            definition = (
                db.query(EngineRuleSetDefinition)
                .filter(EngineRuleSetDefinition.id == definition.id)
                .first()
            )
            if definition is None:
                raise GuardrailViolation(
                    "RuleSet baseline não encontrado após criação de versão.",
                    details={"reason": "baseline_ruleset_reload_failed"},
                    hint="Tente novamente ou consulte logs de auditoria.",
                )
            created_now = True

    if definition.published_version is None:
        version_record = get_ruleset_version(
            db,
            tenant_id=tenant_id,
            ruleset_id=definition.id,
            version=candidate_version,
            include_archived=True,
        )
        if version_record.version.status not in {
            RuleSetVersionStatus.VALIDATED.value,
            RuleSetVersionStatus.PUBLISHED.value,
        }:
            validate_ruleset_version(
                db,
                tenant_id=tenant_id,
                ruleset_id=definition.id,
                version=candidate_version,
                column_types=column_types,
                validated_by=actor,
            )
        publish_ruleset_version(
            db,
            tenant_id=tenant_id,
            ruleset_id=definition.id,
            version=candidate_version,
            published_by=actor,
        )
        definition = (
            db.query(EngineRuleSetDefinition)
            .filter(EngineRuleSetDefinition.id == definition.id)
            .first()
        )
        if definition is None:
            raise GuardrailViolation(
                "RuleSet baseline não encontrado após publicação.",
                details={"reason": "baseline_ruleset_reload_failed"},
                hint="Tente novamente ou consulte logs de auditoria.",
            )

    published_version = int(definition.published_version or definition.active_version)
    state_row = _upsert_migration_row(
        db,
        tenant_id=tenant_id,
        defaults={
            "ruleset_id": definition.id,
            "ruleset_name": definition.name,
            "baseline_version": published_version,
            "updated_by": actor,
            "phase": _default_phase().value,
            "parity_target_percent": _parity_target_percent(),
        },
    )
    state_row.ruleset_id = definition.id
    state_row.ruleset_name = definition.name
    state_row.baseline_version = published_version
    state_row.updated_by = actor
    state_row.updated_at = datetime.now(UTC)

    _audit(
        db,
        tenant_id=tenant_id,
        actor=actor,
        action="CLASSIFICATION_MIGRATION_BASELINE_BOOTSTRAP",
        details={
            "ruleset_id": definition.id,
            "ruleset_name": definition.name,
            "published_version": published_version,
            "created_now": created_now,
            "stale_days": resolved_stale_days,
            "legacy_os_definitions": resolved_legacy_terms,
        },
    )
    db.commit()

    return {
        "ruleset_id": definition.id,
        "ruleset_name": definition.name,
        "published_version": published_version,
        "created_now": created_now,
        "phase": state_row.phase,
    }


def ensure_baseline_ruleset_for_all_tenants(
    db: Session,
    *,
    actor: str | None,
    ruleset_name: str | None = None,
    stale_days: int | None = None,
    legacy_os_definitions: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    tenants = (
        db.query(Tenant)
        .filter(Tenant.is_active.is_(True))
        .order_by(Tenant.created_at.asc())
        .all()
    )
    results: list[dict[str, Any]] = []
    for tenant in tenants:
        result = ensure_baseline_ruleset_for_tenant(
            db,
            tenant_id=tenant.id,
            actor=actor,
            ruleset_name=ruleset_name,
            stale_days=stale_days,
            legacy_os_definitions=legacy_os_definitions,
        )
        results.append({"tenant_id": tenant.id, **result})
    return results


def get_classification_migration_state(
    db: Session,
    *,
    tenant_id: str,
) -> ClassificationMigrationState:
    row = (
        db.query(EngineClassificationMigration)
        .filter(EngineClassificationMigration.tenant_id == tenant_id)
        .first()
    )
    if row is None:
        return ClassificationMigrationState(
            tenant_id=tenant_id,
            phase=_default_phase(),
            ruleset_id=None,
            ruleset_name=engine_settings.classification_default_ruleset_name,
            baseline_version=None,
            parity_target_percent=_parity_target_percent(),
            last_parity_percent=None,
            last_parity_passed=None,
            last_dataset_version_id=None,
            last_run_id=None,
            updated_at=None,
            updated_by=None,
            is_default=True,
        )
    return ClassificationMigrationState(
        tenant_id=row.tenant_id,
        phase=ClassificationMigrationPhase(row.phase),
        ruleset_id=row.ruleset_id,
        ruleset_name=row.ruleset_name,
        baseline_version=row.baseline_version,
        parity_target_percent=float(row.parity_target_percent),
        last_parity_percent=float(row.last_parity_percent)
        if row.last_parity_percent is not None
        else None,
        last_parity_passed=row.last_parity_passed,
        last_dataset_version_id=row.last_dataset_version_id,
        last_run_id=row.last_run_id,
        updated_at=row.updated_at,
        updated_by=row.updated_by,
        is_default=False,
    )


def promote_classification_migration_phase(
    db: Session,
    *,
    tenant_id: str,
    target_phase: ClassificationMigrationPhase,
    updated_by: str | None,
    enforce_parity: bool = True,
) -> ClassificationMigrationState:
    current = get_classification_migration_state(db, tenant_id=tenant_id)
    _validate_phase_transition(current.phase, target_phase)
    if enforce_parity and target_phase in {
        ClassificationMigrationPhase.B,
        ClassificationMigrationPhase.C,
        ClassificationMigrationPhase.D,
    }:
        if not current.last_parity_passed:
            raise GuardrailViolation(
                "Paridade mínima ainda não atingida para promover a fase.",
                details={
                    "reason": "parity_gate_failed",
                    "current_phase": current.phase.value,
                    "target_phase": target_phase.value,
                    "last_parity_percent": current.last_parity_percent,
                    "parity_target_percent": current.parity_target_percent,
                },
                hint="Execute shadow parity e só promova após atingir >= 99.9%.",
            )

    row = _upsert_migration_row(
        db,
        tenant_id=tenant_id,
        defaults={
            "phase": target_phase.value,
            "parity_target_percent": _parity_target_percent(),
            "updated_by": updated_by,
        },
    )
    previous_phase = row.phase
    row.phase = target_phase.value
    row.updated_by = updated_by
    row.updated_at = datetime.now(UTC)

    _audit(
        db,
        tenant_id=tenant_id,
        actor=updated_by,
        action="CLASSIFICATION_MIGRATION_PHASE_PROMOTE",
        details={
            "previous_phase": previous_phase,
            "target_phase": target_phase.value,
            "enforce_parity": enforce_parity,
        },
    )
    db.commit()
    return get_classification_migration_state(db, tenant_id=tenant_id)


def build_shadow_parity_report(
    db: Session,
    *,
    tenant_id: str,
    dataset_version_id: str,
    run_id: str | None = None,
    limit: int = 10_000,
    persist_snapshot: bool = True,
    actor: str | None = None,
) -> dict[str, Any]:
    max_limit = max(1, min(limit, 100_000))
    run = _resolve_shadow_run(
        db,
        tenant_id=tenant_id,
        dataset_version_id=dataset_version_id,
        run_id=run_id,
    )
    if run is None:
        raise GuardrailViolation(
            "Nenhuma execução shadow encontrada para este dataset_version.",
            details={
                "reason": "shadow_run_not_found",
                "dataset_version_id": dataset_version_id,
                "run_id": run_id,
            },
            hint="Execute materialização em mode=shadow para gerar evidências de paridade.",
        )

    metrics = _extract_classification_metrics(run.metrics_json)
    rows_classified = _safe_int(metrics.get("rows_classified"))
    reported_divergences = _safe_int(metrics.get("divergences"))

    query = db.query(EngineClassificationDivergence).filter(
        EngineClassificationDivergence.tenant_id == tenant_id,
        EngineClassificationDivergence.dataset_version_id == dataset_version_id,
    )
    if run.id:
        query = query.filter(EngineClassificationDivergence.run_id == run.id)

    divergence_rows = query.order_by(EngineClassificationDivergence.created_at.desc()).all()
    total_rows = len(divergence_rows)
    truncated_rows = divergence_rows[:max_limit]

    severity_counter: Counter[str] = Counter()
    dimension_counter: Counter[str] = Counter()
    rule_counter: Counter[str] = Counter()
    severity_map = status_severity_by_key()

    for row in truncated_rows:
        diff = _load_json(row.diff_json, fallback={})
        dimension = _infer_divergence_dimension(diff)
        severity = _infer_divergence_severity(
            diff=diff,
            row=row,
            severity_by_status=severity_map,
        )
        rule_keys = _extract_rule_keys(diff)

        dimension_counter[dimension] += 1
        severity_counter[severity] += 1
        rule_counter.update(rule_keys)

    total_divergences = reported_divergences if reported_divergences is not None else total_rows
    parity_percent: float | None = None
    parity_ok: bool | None = None
    if rows_classified and rows_classified > 0:
        parity_percent = round(
            max(0.0, ((rows_classified - max(total_divergences, 0)) / rows_classified) * 100.0),
            4,
        )
        parity_ok = parity_percent >= _parity_target_percent()

    report = {
        "tenant_id": tenant_id,
        "dataset_version_id": dataset_version_id,
        "run_id": run.id,
        "ruleset_name": metrics.get("ruleset_name"),
        "rows_classified": rows_classified,
        "rows_scanned": _safe_int(metrics.get("rows_scanned")),
        "total_divergences": total_divergences,
        "observed_divergence_rows": total_rows,
        "report_rows": len(truncated_rows),
        "report_truncated": total_rows > len(truncated_rows),
        "parity_percent": parity_percent,
        "parity_target_percent": _parity_target_percent(),
        "parity_ok": parity_ok,
        "by_dimension": dict(dimension_counter),
        "by_severity": dict(severity_counter),
        "by_rule": dict(rule_counter),
    }

    if persist_snapshot:
        row = _upsert_migration_row(
            db,
            tenant_id=tenant_id,
            defaults={
                "phase": _default_phase().value,
                "parity_target_percent": _parity_target_percent(),
                "updated_by": actor,
            },
        )
        row.last_dataset_version_id = dataset_version_id
        row.last_run_id = run.id
        row.last_parity_percent = parity_percent
        row.last_parity_passed = parity_ok
        row.updated_by = actor
        row.updated_at = datetime.now(UTC)
        _audit(
            db,
            tenant_id=tenant_id,
            actor=actor,
            action="CLASSIFICATION_MIGRATION_PARITY_REPORT",
            details={
                "dataset_version_id": dataset_version_id,
                "run_id": run.id,
                "parity_percent": parity_percent,
                "parity_target_percent": _parity_target_percent(),
                "parity_ok": parity_ok,
                "total_divergences": total_divergences,
            },
        )
        db.commit()

    return report


def get_legacy_rule_inventory(
    *,
    stale_days: int | None = None,
    legacy_os_definitions: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    resolved_stale_days = int(stale_days or engine_settings.classification_stale_days)
    resolved_legacy_terms = _resolve_legacy_os_terms(legacy_os_definitions)
    return list_legacy_rule_inventory(
        stale_days=resolved_stale_days,
        legacy_os_definitions=resolved_legacy_terms,
    )


def _resolve_shadow_run(
    db: Session,
    *,
    tenant_id: str,
    dataset_version_id: str,
    run_id: str | None,
) -> EngineRun | None:
    query = db.query(EngineRun).filter(
        EngineRun.tenant_id == tenant_id,
        EngineRun.dataset_version_id == dataset_version_id,
        EngineRun.run_type == "materialize",
    )
    if run_id:
        return query.filter(EngineRun.id == run_id).first()

    runs = query.order_by(EngineRun.started_at.desc()).all()
    for run in runs:
        metrics = _extract_classification_metrics(run.metrics_json)
        if str(metrics.get("mode", "")).lower() == "shadow":
            return run
    return None


def _extract_classification_metrics(raw: str | None) -> dict[str, Any]:
    payload = _load_json(raw, fallback={})
    if not isinstance(payload, dict):
        return {}
    nested = payload.get("classification")
    if isinstance(nested, dict):
        return nested
    if "mode" in payload and "rows_classified" in payload:
        return payload
    return {}


def _load_json(raw: str | None, *, fallback: Any) -> Any:
    if not raw:
        return fallback
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return fallback


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _infer_divergence_dimension(diff: dict[str, Any]) -> str:
    keys = [key for key in ("primary_status", "primary_status_label", "flags") if key in diff]
    if len(keys) == 1:
        return keys[0]
    if len(keys) > 1:
        return "mixed"
    return "unknown"


def _infer_divergence_severity(
    *,
    diff: dict[str, Any],
    row: EngineClassificationDivergence,
    severity_by_status: dict[str, str],
) -> str:
    explicit = diff.get("severity")
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip().upper()

    candidates: list[str] = []
    primary_diff = diff.get("primary_status")
    if isinstance(primary_diff, dict):
        for key in ("legacy", "declarative"):
            status = primary_diff.get(key)
            if isinstance(status, str) and status in severity_by_status:
                candidates.append(severity_by_status[status])
    else:
        for status in (row.legacy_primary_status, row.declarative_primary_status):
            if isinstance(status, str) and status in severity_by_status:
                candidates.append(severity_by_status[status])

    flag_diff = diff.get("flags")
    if isinstance(flag_diff, dict):
        for flag_list in (flag_diff.get("legacy"), flag_diff.get("declarative")):
            if isinstance(flag_list, list):
                for status in flag_list:
                    if isinstance(status, str) and status in severity_by_status:
                        candidates.append(severity_by_status[status])

    if not candidates:
        return "UNKNOWN"
    return _max_severity(candidates)


def _max_severity(values: Sequence[str]) -> str:
    rank = {"DANGER": 4, "WARNING": 3, "INFO": 2, "SUCCESS": 1, "UNKNOWN": 0}
    best = "UNKNOWN"
    for value in values:
        normalized = value.strip().upper()
        if rank.get(normalized, 0) > rank.get(best, 0):
            best = normalized
    return best


def _extract_rule_keys(diff: dict[str, Any]) -> list[str]:
    raw = diff.get("declarative_rule_keys")
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, str) and item.strip()]


def _validate_phase_transition(
    current_phase: ClassificationMigrationPhase,
    target_phase: ClassificationMigrationPhase,
) -> None:
    order = {
        ClassificationMigrationPhase.A: 1,
        ClassificationMigrationPhase.B: 2,
        ClassificationMigrationPhase.C: 3,
        ClassificationMigrationPhase.D: 4,
    }
    if target_phase == current_phase:
        return
    if order[target_phase] < order[current_phase]:
        raise GuardrailViolation(
            "Regressão de fase não é permitida no endpoint de promoção.",
            details={
                "reason": "migration_phase_regression_not_allowed",
                "current_phase": current_phase.value,
                "target_phase": target_phase.value,
            },
            hint="Use rollback instantâneo alterando o mode para legacy.",
        )
    if order[target_phase] - order[current_phase] > 1:
        raise GuardrailViolation(
            "Promoção de fase inválida (pulo detectado).",
            details={
                "reason": "migration_phase_skip_not_allowed",
                "current_phase": current_phase.value,
                "target_phase": target_phase.value,
            },
            hint="Promova em sequência: A -> B -> C -> D.",
        )


def _resolve_ruleset_name(explicit_name: str | None) -> str:
    candidate = (explicit_name or engine_settings.classification_default_ruleset_name or "").strip()
    if candidate:
        return candidate
    raise GuardrailViolation(
        "Nome do RuleSet baseline ausente.",
        details={"reason": "baseline_ruleset_name_missing"},
        hint="Defina ENGINE_CLASSIFICATION_DEFAULT_RULESET_NAME ou envie ruleset_name.",
    )


def _resolve_legacy_os_terms(explicit_terms: Sequence[str] | None) -> list[str]:
    if explicit_terms:
        return [item.strip() for item in explicit_terms if isinstance(item, str) and item.strip()]
    configured = engine_settings.classification_legacy_os_definitions
    if isinstance(configured, str) and configured.strip():
        return [item.strip() for item in configured.split(",") if item.strip()]
    return []


def _default_phase() -> ClassificationMigrationPhase:
    raw = (engine_settings.classification_migration_default_phase or "A").strip().upper()
    for phase in ClassificationMigrationPhase:
        if phase.value == raw:
            return phase
    return ClassificationMigrationPhase.A


def _parity_target_percent() -> float:
    return float(engine_settings.classification_migration_parity_threshold_percent or 99.9)


def _upsert_migration_row(
    db: Session,
    *,
    tenant_id: str,
    defaults: dict[str, Any],
) -> EngineClassificationMigration:
    row = (
        db.query(EngineClassificationMigration)
        .filter(EngineClassificationMigration.tenant_id == tenant_id)
        .first()
    )
    if row is None:
        row = EngineClassificationMigration(tenant_id=tenant_id, **defaults)
        db.add(row)
        db.flush()
    return row


def _audit(
    db: Session,
    *,
    tenant_id: str,
    actor: str | None,
    action: str,
    details: dict[str, Any] | None = None,
) -> None:
    db.add(
        AuditLog(
            tenant_id=tenant_id,
            dataset_version_id=None,
            actor=actor or "system",
            action=action,
            entity_type="engine_classification_migration",
            entity_id=tenant_id,
            details=json.dumps(details or {}, ensure_ascii=False, default=str),
        )
    )
