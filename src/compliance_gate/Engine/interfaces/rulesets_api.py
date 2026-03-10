from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy.orm import Session

from compliance_gate.authentication.http.dependencies import require_role
from compliance_gate.authentication.models import Role, User
from compliance_gate.Engine.config.engine_settings import engine_settings
from compliance_gate.Engine.errors import DeclarativeEngineError, InvalidExpressionSyntax
from compliance_gate.Engine.expressions import ExpressionValidationOptions
from compliance_gate.Engine.rulesets import (
    ClassificationMigrationPhase,
    ClassificationRuntimeMode,
    RuleSetPayloadV2,
    RuleSetRecord,
    RuleSetValidationResult,
    RuleSetVersionRecord,
    RuleSetVersionStatus,
    archive_ruleset,
    build_shadow_parity_report,
    compile_ruleset_from_payload,
    create_ruleset,
    create_ruleset_version,
    dry_run_ruleset,
    ensure_baseline_ruleset_for_all_tenants,
    ensure_baseline_ruleset_for_tenant,
    explain_row,
    explain_sample,
    get_classification_migration_state,
    get_legacy_rule_inventory,
    get_classification_mode_state,
    get_ruleset,
    get_ruleset_by_name,
    get_ruleset_version,
    list_recent_classification_runs,
    list_recent_divergences,
    list_ruleset_versions,
    list_rulesets,
    publish_ruleset_version,
    promote_classification_migration_phase,
    rollback_ruleset,
    set_classification_mode,
    update_ruleset,
    update_ruleset_version,
    validate_ruleset_payload,
    validate_ruleset_version,
)
from compliance_gate.infra.db.session import get_db
from compliance_gate.shared.schemas.responses import ApiResponse

router = APIRouter(prefix="/engine", tags=["Engine RuleSets"])


class RuleSetCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=256)
    description: str | None = None
    payload: RuleSetPayloadV2


class RuleSetUpdateRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=256)
    description: str | None = None


class RuleSetVersionCreateRequest(BaseModel):
    source_version: int | None = Field(default=None, ge=1)
    payload: RuleSetPayloadV2 | None = None


class RuleSetVersionUpdateRequest(BaseModel):
    payload: RuleSetPayloadV2


class RuleSetValidateRequest(BaseModel):
    column_types: dict[str, str] = Field(default_factory=dict)


class RuleSetRollbackRequest(BaseModel):
    target_version: int | None = Field(default=None, ge=1)


class RuleSetValidatePayloadRequest(BaseModel):
    payload: dict[str, Any]
    column_types: dict[str, str] = Field(default_factory=dict)


class RuleSetExplainRowRequest(BaseModel):
    payload: dict[str, Any]
    row: dict[str, Any]
    ruleset_name: str = Field(default="preview-ruleset", min_length=1, max_length=256)
    version: int = Field(default=0, ge=0)


class RuleSetExplainSampleRequest(BaseModel):
    payload: dict[str, Any]
    rows: list[dict[str, Any]] = Field(min_length=1)
    limit: int | None = Field(default=None, ge=1)
    ruleset_name: str = Field(default="preview-ruleset", min_length=1, max_length=256)
    version: int = Field(default=0, ge=0)


class RuleSetDryRunRequest(BaseModel):
    payload: dict[str, Any]
    rows: list[dict[str, Any]] = Field(min_length=1)
    mode: ClassificationRuntimeMode = ClassificationRuntimeMode.DECLARATIVE
    explain_sample_limit: int = Field(default=5, ge=1, le=100)
    ruleset_name: str = Field(default="preview-ruleset", min_length=1, max_length=256)
    version: int = Field(default=0, ge=0)


class RuleSetVersionResponse(BaseModel):
    version: int
    status: RuleSetVersionStatus
    created_at: datetime
    created_by: str | None = None
    validated_at: datetime | None = None
    validated_by: str | None = None
    published_at: datetime | None = None
    published_by: str | None = None
    payload: RuleSetPayloadV2


class RuleSetResponse(BaseModel):
    id: str
    tenant_id: str
    name: str
    description: str | None = None
    created_by: str | None = None
    created_at: datetime
    updated_at: datetime | None = None
    active_version: int
    active_status: RuleSetVersionStatus
    published_version: int | None = None
    is_archived: bool
    active_payload: RuleSetPayloadV2


class RuleSetDetailResponse(RuleSetResponse):
    versions: list[RuleSetVersionResponse]


class RuleSetResolveResponse(BaseModel):
    ruleset_id: str
    ruleset_name: str
    tenant_id: str
    resolved_as: str
    version: RuleSetVersionResponse


class RuleSetValidateResponse(BaseModel):
    version: RuleSetVersionResponse
    validation: RuleSetValidationResult


class ClassificationModeUpdateRequest(BaseModel):
    mode: ClassificationRuntimeMode
    ruleset_name: str | None = Field(default=None, min_length=1, max_length=256)


class ClassificationModeResponse(BaseModel):
    mode: ClassificationRuntimeMode
    ruleset_name: str | None = None
    source: str
    updated_at: datetime | None = None
    updated_by: str | None = None


class ClassificationMigrationStateResponse(BaseModel):
    phase: ClassificationMigrationPhase
    ruleset_id: str | None = None
    ruleset_name: str | None = None
    baseline_version: int | None = None
    parity_target_percent: float
    last_parity_percent: float | None = None
    last_parity_passed: bool | None = None
    last_dataset_version_id: str | None = None
    last_run_id: str | None = None
    updated_at: datetime | None = None
    updated_by: str | None = None
    source: str


class ClassificationMigrationBootstrapRequest(BaseModel):
    ruleset_name: str | None = Field(default=None, min_length=1, max_length=256)
    stale_days: int | None = Field(default=None, ge=1, le=3650)
    legacy_os_definitions: list[str] | None = None
    all_tenants: bool = False


class ClassificationMigrationBootstrapResponse(BaseModel):
    tenant_id: str
    ruleset_id: str
    ruleset_name: str
    published_version: int
    created_now: bool
    phase: ClassificationMigrationPhase


class ClassificationMigrationPromoteRequest(BaseModel):
    target_phase: ClassificationMigrationPhase
    enforce_parity: bool = True


class ShadowParityReportResponse(BaseModel):
    tenant_id: str
    dataset_version_id: str
    run_id: str | None = None
    ruleset_name: str | None = None
    rows_classified: int | None = None
    rows_scanned: int | None = None
    total_divergences: int
    observed_divergence_rows: int
    report_rows: int
    report_truncated: bool
    parity_percent: float | None = None
    parity_target_percent: float
    parity_ok: bool | None = None
    by_dimension: dict[str, int] = Field(default_factory=dict)
    by_severity: dict[str, int] = Field(default_factory=dict)
    by_rule: dict[str, int] = Field(default_factory=dict)


class LegacyRuleInventoryResponse(BaseModel):
    rule_key: str
    block_kind: str
    precedence: int
    legacy_module: str
    status_key: str
    status_label: str
    severity: str
    is_flag: bool
    description: str
    condition: dict[str, Any]
    output: dict[str, Any]


class ClassificationDivergenceResponse(BaseModel):
    id: str
    tenant_id: str
    dataset_version_id: str | None = None
    run_id: str | None = None
    ruleset_name: str | None = None
    machine_id: str | None = None
    hostname: str | None = None
    legacy_primary_status: str | None = None
    legacy_primary_status_label: str | None = None
    legacy_flags: list[str] = Field(default_factory=list)
    declarative_primary_status: str | None = None
    declarative_primary_status_label: str | None = None
    declarative_flags: list[str] = Field(default_factory=list)
    divergence_kind: str = "unknown"
    severity: str = "UNKNOWN"
    rule_keys: list[str] = Field(default_factory=list)
    diff: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class ClassificationRunMetricsResponse(BaseModel):
    run_id: str
    tenant_id: str
    dataset_version_id: str | None = None
    status: str
    started_at: datetime | None = None
    ended_at: datetime | None = None
    mode: ClassificationRuntimeMode | None = None
    cutover_phase: ClassificationMigrationPhase | None = None
    migration_phase: ClassificationMigrationPhase | None = None
    ruleset_name: str | None = None
    rows_scanned: int | None = None
    rows_classified: int | None = None
    elapsed_ms: float | None = None
    rule_hits: dict[str, int] = Field(default_factory=dict)
    divergences: int | None = None
    error_truncated: str | None = None


def _err_status(exc: DeclarativeEngineError) -> int:
    reason = exc.details.get("reason")
    if isinstance(reason, str) and reason.endswith("_not_found"):
        return 404
    if reason in {"ruleset_archived"}:
        return 410
    if reason in {"unique_violation"}:
        return 409
    return 400


def _raise_declarative(exc: DeclarativeEngineError) -> None:
    raise HTTPException(status_code=_err_status(exc), detail=exc.to_dict()) from exc


def _validation_options() -> ExpressionValidationOptions:
    return ExpressionValidationOptions(
        max_nodes=engine_settings.expression_max_nodes,
        max_depth=engine_settings.expression_max_depth,
    )


def _to_ruleset_response(record: RuleSetRecord) -> RuleSetResponse:
    definition = record.definition
    version = record.version
    return RuleSetResponse(
        id=definition.id,
        tenant_id=definition.tenant_id,
        name=definition.name,
        description=definition.description,
        created_by=definition.created_by,
        created_at=definition.created_at,
        updated_at=definition.updated_at,
        active_version=definition.active_version,
        active_status=RuleSetVersionStatus(version.status),
        published_version=definition.published_version,
        is_archived=definition.is_archived,
        active_payload=record.payload,
    )


def _to_version_response(record: RuleSetVersionRecord) -> RuleSetVersionResponse:
    version = record.version
    return RuleSetVersionResponse(
        version=version.version,
        status=RuleSetVersionStatus(version.status),
        created_at=version.created_at,
        created_by=version.created_by,
        validated_at=version.validated_at,
        validated_by=version.validated_by,
        published_at=version.published_at,
        published_by=version.published_by,
        payload=record.payload,
    )


def _to_ruleset_detail(
    record: RuleSetRecord,
    version_records: list[RuleSetVersionRecord],
) -> RuleSetDetailResponse:
    return RuleSetDetailResponse(
        **_to_ruleset_response(record).model_dump(),
        versions=[_to_version_response(version) for version in version_records],
    )


def _to_mode_response(
    *,
    mode: ClassificationRuntimeMode,
    ruleset_name: str | None,
    source: str,
    updated_at: datetime | None,
    updated_by: str | None,
) -> ClassificationModeResponse:
    return ClassificationModeResponse(
        mode=mode,
        ruleset_name=ruleset_name,
        source=source,
        updated_at=updated_at,
        updated_by=updated_by,
    )


def _to_migration_state_response(state) -> ClassificationMigrationStateResponse:
    return ClassificationMigrationStateResponse(
        phase=state.phase,
        ruleset_id=state.ruleset_id,
        ruleset_name=state.ruleset_name,
        baseline_version=state.baseline_version,
        parity_target_percent=state.parity_target_percent,
        last_parity_percent=state.last_parity_percent,
        last_parity_passed=state.last_parity_passed,
        last_dataset_version_id=state.last_dataset_version_id,
        last_run_id=state.last_run_id,
        updated_at=state.updated_at,
        updated_by=state.updated_by,
        source="default" if state.is_default else "configured",
    )


def _load_json(raw: str | None, *, fallback: Any) -> Any:
    if not raw:
        return fallback
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return fallback


def _safe_rule_hits(raw: Any) -> dict[str, int]:
    if not isinstance(raw, dict):
        return {}

    sanitized: dict[str, int] = {}
    for key, value in raw.items():
        if not isinstance(key, str):
            continue
        try:
            sanitized[key] = int(value)
        except (TypeError, ValueError):
            continue
    return sanitized


def _loc_to_node_path(loc: tuple[Any, ...]) -> str | None:
    if not loc:
        return None

    parts: list[str] = []
    for item in loc:
        if isinstance(item, int):
            if parts:
                parts[-1] = f"{parts[-1]}[{item}]"
            else:
                parts.append(f"[{item}]")
            continue
        token = str(item)
        if token:
            parts.append(token)

    if not parts:
        return None
    return ".".join(parts)


def _parse_ruleset_payload(raw_payload: dict[str, Any]) -> RuleSetPayloadV2:
    try:
        return RuleSetPayloadV2.model_validate(raw_payload)
    except ValidationError as exc:
        errors = exc.errors(include_url=False)
        node_path = _loc_to_node_path(tuple(errors[0].get("loc", ()))) if errors else None
        raise InvalidExpressionSyntax(
            "Payload do RuleSet inválido.",
            details={
                "reason": "invalid_ruleset_payload",
                "errors": errors,
            },
            hint="Revise schema_version, blocks, entries, condition e output.",
            node_path=node_path,
        ) from exc


def _extract_classification_metrics(raw: str | None) -> dict[str, Any]:
    payload = _load_json(raw, fallback={})
    if not isinstance(payload, dict):
        return {}

    nested = payload.get("classification")
    if isinstance(nested, dict):
        return nested

    if "mode" not in payload and "rule_hits" not in payload and "rows_scanned" not in payload:
        return {}

    return payload


def _to_divergence_response(row) -> ClassificationDivergenceResponse:
    legacy_flags = _load_json(row.legacy_flags_json, fallback=[])
    declarative_flags = _load_json(row.declarative_flags_json, fallback=[])
    diff = _load_json(row.diff_json, fallback={})
    if not isinstance(legacy_flags, list):
        legacy_flags = []
    if not isinstance(declarative_flags, list):
        declarative_flags = []
    if not isinstance(diff, dict):
        diff = {}
    rule_keys = diff.get("declarative_rule_keys")
    if not isinstance(rule_keys, list):
        rule_keys = []
    divergence_kind = diff.get("divergence_kind")
    if not isinstance(divergence_kind, str) or not divergence_kind.strip():
        divergence_kind = "unknown"
    severity = diff.get("severity")
    if not isinstance(severity, str) or not severity.strip():
        severity = "UNKNOWN"

    return ClassificationDivergenceResponse(
        id=row.id,
        tenant_id=row.tenant_id,
        dataset_version_id=row.dataset_version_id,
        run_id=row.run_id,
        ruleset_name=row.ruleset_name,
        machine_id=row.machine_id,
        hostname=row.hostname,
        legacy_primary_status=row.legacy_primary_status,
        legacy_primary_status_label=row.legacy_primary_status_label,
        legacy_flags=[str(item) for item in legacy_flags if isinstance(item, str)],
        declarative_primary_status=row.declarative_primary_status,
        declarative_primary_status_label=row.declarative_primary_status_label,
        declarative_flags=[str(item) for item in declarative_flags if isinstance(item, str)],
        divergence_kind=divergence_kind,
        severity=severity,
        rule_keys=[str(item) for item in rule_keys if isinstance(item, str)],
        diff=diff,
        created_at=row.created_at,
    )


def _parse_mode(raw: Any) -> ClassificationRuntimeMode | None:
    if not isinstance(raw, str):
        return None
    normalized = raw.strip().lower()
    for mode in ClassificationRuntimeMode:
        if mode.value == normalized:
            return mode
    return None


def _parse_migration_phase(raw: Any) -> ClassificationMigrationPhase | None:
    if not isinstance(raw, str):
        return None
    normalized = raw.strip().upper()
    for phase in ClassificationMigrationPhase:
        if phase.value == normalized:
            return phase
    return None


def _to_run_metrics_response(run) -> ClassificationRunMetricsResponse:
    metrics = _extract_classification_metrics(run.metrics_json)
    return ClassificationRunMetricsResponse(
        run_id=run.id,
        tenant_id=run.tenant_id,
        dataset_version_id=run.dataset_version_id,
        status=run.status,
        started_at=run.started_at,
        ended_at=run.ended_at,
        mode=_parse_mode(metrics.get("mode")),
        cutover_phase=_parse_migration_phase(metrics.get("cutover_phase")),
        migration_phase=_parse_migration_phase(metrics.get("migration_phase")),
        ruleset_name=metrics.get("ruleset_name"),
        rows_scanned=metrics.get("rows_scanned"),
        rows_classified=metrics.get("rows_classified"),
        elapsed_ms=metrics.get("elapsed_ms"),
        rule_hits=_safe_rule_hits(metrics.get("rule_hits")),
        divergences=metrics.get("divergences"),
        error_truncated=run.error_truncated,
    )


@router.post("/rulesets", response_model=ApiResponse[RuleSetResponse])
def post_ruleset(
    body: RuleSetCreateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    try:
        record = create_ruleset(
            db,
            tenant_id=current_user.tenant_id,
            name=body.name,
            description=body.description,
            created_by=current_user.id,
            payload=body.payload,
        )
        return ApiResponse(data=_to_ruleset_response(record))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.get("/rulesets", response_model=ApiResponse[list[RuleSetResponse]])
def get_rulesets(
    include_archived: bool = Query(default=False),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    try:
        records = list_rulesets(
            db,
            tenant_id=current_user.tenant_id,
            include_archived=include_archived,
        )
        return ApiResponse(data=[_to_ruleset_response(record) for record in records])
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.get("/rulesets/{ruleset_id}", response_model=ApiResponse[RuleSetDetailResponse])
def get_ruleset_by_id(
    ruleset_id: str,
    include_archived: bool = Query(default=False),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    try:
        record = get_ruleset(
            db,
            tenant_id=current_user.tenant_id,
            ruleset_id=ruleset_id,
            include_archived=include_archived,
        )
        versions = list_ruleset_versions(
            db,
            tenant_id=current_user.tenant_id,
            ruleset_id=ruleset_id,
            include_archived=True,
        )
        return ApiResponse(data=_to_ruleset_detail(record, versions))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.put("/rulesets/{ruleset_id}", response_model=ApiResponse[RuleSetResponse])
def put_ruleset(
    ruleset_id: str,
    body: RuleSetUpdateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    try:
        record = update_ruleset(
            db,
            tenant_id=current_user.tenant_id,
            ruleset_id=ruleset_id,
            name=body.name,
            description=body.description,
            updated_by=current_user.id,
        )
        return ApiResponse(data=_to_ruleset_response(record))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.delete("/rulesets/{ruleset_id}", response_model=ApiResponse[RuleSetResponse])
def delete_ruleset(
    ruleset_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    try:
        record = archive_ruleset(
            db,
            tenant_id=current_user.tenant_id,
            ruleset_id=ruleset_id,
            archived_by=current_user.id,
        )
        return ApiResponse(data=_to_ruleset_response(record))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.get(
    "/rulesets/{ruleset_id}/versions", response_model=ApiResponse[list[RuleSetVersionResponse]]
)
def get_ruleset_versions(
    ruleset_id: str,
    include_archived: bool = Query(default=True),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    try:
        versions = list_ruleset_versions(
            db,
            tenant_id=current_user.tenant_id,
            ruleset_id=ruleset_id,
            include_archived=include_archived,
        )
        return ApiResponse(data=[_to_version_response(version) for version in versions])
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.get(
    "/rulesets/{ruleset_id}/versions/{version}",
    response_model=ApiResponse[RuleSetVersionResponse],
)
def get_ruleset_version_by_number(
    ruleset_id: str,
    version: int,
    include_archived: bool = Query(default=True),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    try:
        record = get_ruleset_version(
            db,
            tenant_id=current_user.tenant_id,
            ruleset_id=ruleset_id,
            version=version,
            include_archived=include_archived,
        )
        return ApiResponse(data=_to_version_response(record))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.post(
    "/rulesets/{ruleset_id}/versions",
    response_model=ApiResponse[RuleSetVersionResponse],
)
def post_ruleset_version(
    ruleset_id: str,
    body: RuleSetVersionCreateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    try:
        record = create_ruleset_version(
            db,
            tenant_id=current_user.tenant_id,
            ruleset_id=ruleset_id,
            created_by=current_user.id,
            source_version=body.source_version,
            payload=body.payload,
        )
        return ApiResponse(data=_to_version_response(record))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.put(
    "/rulesets/{ruleset_id}/versions/{version}",
    response_model=ApiResponse[RuleSetVersionResponse],
)
def put_ruleset_version(
    ruleset_id: str,
    version: int,
    body: RuleSetVersionUpdateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    try:
        record = update_ruleset_version(
            db,
            tenant_id=current_user.tenant_id,
            ruleset_id=ruleset_id,
            version=version,
            payload=body.payload,
            updated_by=current_user.id,
        )
        return ApiResponse(data=_to_version_response(record))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.post(
    "/rulesets/{ruleset_id}/versions/{version}/validate",
    response_model=ApiResponse[RuleSetValidateResponse],
)
def post_ruleset_validate(
    ruleset_id: str,
    version: int,
    body: RuleSetValidateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    if not body.column_types:
        raise HTTPException(status_code=400, detail="column_types não pode ser vazio")

    try:
        record, validation = validate_ruleset_version(
            db,
            tenant_id=current_user.tenant_id,
            ruleset_id=ruleset_id,
            version=version,
            column_types=body.column_types,
            validated_by=current_user.id,
            options=_validation_options(),
        )
        return ApiResponse(
            data=RuleSetValidateResponse(
                version=_to_version_response(record),
                validation=validation,
            )
        )
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.post(
    "/rulesets/{ruleset_id}/versions/{version}/publish",
    response_model=ApiResponse[RuleSetVersionResponse],
)
def post_ruleset_publish(
    ruleset_id: str,
    version: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    try:
        record = publish_ruleset_version(
            db,
            tenant_id=current_user.tenant_id,
            ruleset_id=ruleset_id,
            version=version,
            published_by=current_user.id,
        )
        return ApiResponse(data=_to_version_response(record))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.post("/rulesets/{ruleset_id}/rollback", response_model=ApiResponse[RuleSetVersionResponse])
def post_ruleset_rollback(
    ruleset_id: str,
    body: RuleSetRollbackRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    try:
        record = rollback_ruleset(
            db,
            tenant_id=current_user.tenant_id,
            ruleset_id=ruleset_id,
            rolled_back_by=current_user.id,
            target_version=body.target_version,
        )
        return ApiResponse(data=_to_version_response(record))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.post("/validate-ruleset", response_model=ApiResponse[dict[str, Any]])
@router.post("/rulesets/validate-ruleset", response_model=ApiResponse[dict[str, Any]])
def post_validate_ruleset_payload(
    body: RuleSetValidatePayloadRequest,
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    del current_user
    try:
        payload = _parse_ruleset_payload(body.payload)
        result = validate_ruleset_payload(
            payload,
            column_types=body.column_types or None,
            options=_validation_options(),
        )
        return ApiResponse(data=result)
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.post("/explain-row", response_model=ApiResponse[dict[str, Any]])
@router.post("/rulesets/explain-row", response_model=ApiResponse[dict[str, Any]])
def post_explain_row(
    body: RuleSetExplainRowRequest,
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    del current_user
    try:
        payload = _parse_ruleset_payload(body.payload)
        compiled = compile_ruleset_from_payload(
            payload,
            ruleset_name=body.ruleset_name,
            version=body.version,
            options=_validation_options(),
        )
        return ApiResponse(data=explain_row(compiled, row=body.row))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.post("/explain-sample", response_model=ApiResponse[dict[str, Any]])
@router.post("/rulesets/explain-sample", response_model=ApiResponse[dict[str, Any]])
def post_explain_sample(
    body: RuleSetExplainSampleRequest,
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    del current_user
    try:
        payload = _parse_ruleset_payload(body.payload)
        compiled = compile_ruleset_from_payload(
            payload,
            ruleset_name=body.ruleset_name,
            version=body.version,
            options=_validation_options(),
        )
        return ApiResponse(data=explain_sample(compiled, rows=body.rows, limit=body.limit))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.post("/dry-run-ruleset", response_model=ApiResponse[dict[str, Any]])
@router.post("/rulesets/dry-run-ruleset", response_model=ApiResponse[dict[str, Any]])
def post_dry_run_ruleset(
    body: RuleSetDryRunRequest,
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    del current_user
    try:
        payload = _parse_ruleset_payload(body.payload)
        compiled = compile_ruleset_from_payload(
            payload,
            ruleset_name=body.ruleset_name,
            version=body.version,
            options=_validation_options(),
        )
        result = dry_run_ruleset(
            compiled,
            rows=body.rows,
            mode=body.mode,
            explain_sample_limit=body.explain_sample_limit,
        )
        return ApiResponse(data=result)
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.get(
    "/internal/rulesets/active",
    response_model=ApiResponse[RuleSetResolveResponse],
)
def get_internal_ruleset_active(
    name: str = Query(..., min_length=1, max_length=256),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    try:
        record = get_ruleset_by_name(
            db,
            tenant_id=current_user.tenant_id,
            name=name,
            resolution="active",
        )
        return ApiResponse(
            data=RuleSetResolveResponse(
                ruleset_id=record.definition.id,
                ruleset_name=record.definition.name,
                tenant_id=record.definition.tenant_id,
                resolved_as="active",
                version=_to_version_response(
                    RuleSetVersionRecord(
                        definition=record.definition,
                        version=record.version,
                        payload=record.payload,
                    )
                ),
            )
        )
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.get(
    "/internal/rulesets/published",
    response_model=ApiResponse[RuleSetResolveResponse],
)
def get_internal_ruleset_published(
    name: str = Query(..., min_length=1, max_length=256),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    try:
        record = get_ruleset_by_name(
            db,
            tenant_id=current_user.tenant_id,
            name=name,
            resolution="published",
        )
        return ApiResponse(
            data=RuleSetResolveResponse(
                ruleset_id=record.definition.id,
                ruleset_name=record.definition.name,
                tenant_id=record.definition.tenant_id,
                resolved_as="published",
                version=_to_version_response(
                    RuleSetVersionRecord(
                        definition=record.definition,
                        version=record.version,
                        payload=record.payload,
                    )
                ),
            )
        )
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.get(
    "/classification/mode",
    response_model=ApiResponse[ClassificationModeResponse],
)
def get_classification_mode_endpoint(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    state = get_classification_mode_state(db, tenant_id=current_user.tenant_id)
    return ApiResponse(
        data=_to_mode_response(
            mode=state.mode,
            ruleset_name=state.ruleset_name,
            source="default" if state.is_default else "configured",
            updated_at=state.updated_at,
            updated_by=state.updated_by,
        )
    )


@router.put(
    "/classification/mode",
    response_model=ApiResponse[ClassificationModeResponse],
)
def put_classification_mode_endpoint(
    body: ClassificationModeUpdateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    try:
        resolved_ruleset_name = body.ruleset_name
        if body.mode in {
            ClassificationRuntimeMode.SHADOW,
            ClassificationRuntimeMode.DECLARATIVE,
        }:
            resolved_ruleset_name = (
                resolved_ruleset_name or engine_settings.classification_default_ruleset_name
            )
            if not resolved_ruleset_name:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "code": "GuardrailViolation",
                        "message": "ruleset_name é obrigatório para mode shadow/declarative.",
                        "details": {"reason": "ruleset_name_required"},
                        "hint": "Informe um RuleSet publicado para o tenant.",
                    },
                )
            get_ruleset_by_name(
                db,
                tenant_id=current_user.tenant_id,
                name=resolved_ruleset_name,
                resolution="published",
            )

        set_classification_mode(
            db,
            tenant_id=current_user.tenant_id,
            mode=body.mode,
            ruleset_name=resolved_ruleset_name,
            updated_by=current_user.id,
        )
        state = get_classification_mode_state(db, tenant_id=current_user.tenant_id)
        return ApiResponse(
            data=_to_mode_response(
                mode=state.mode,
                ruleset_name=state.ruleset_name,
                source="default" if state.is_default else "configured",
                updated_at=state.updated_at,
                updated_by=state.updated_by,
            )
        )
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.get(
    "/classification/migration/state",
    response_model=ApiResponse[ClassificationMigrationStateResponse],
)
def get_classification_migration_state_endpoint(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    state = get_classification_migration_state(db, tenant_id=current_user.tenant_id)
    return ApiResponse(data=_to_migration_state_response(state))


@router.post(
    "/classification/migration/bootstrap-baseline",
    response_model=ApiResponse[list[ClassificationMigrationBootstrapResponse]],
)
def post_classification_migration_bootstrap_baseline(
    body: ClassificationMigrationBootstrapRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    try:
        if body.all_tenants:
            if current_user.tenant_id != engine_settings.default_tenant_id:
                raise HTTPException(
                    status_code=403,
                    detail={
                        "code": "GuardrailViolation",
                        "message": "Bootstrap global de tenants não permitido para este usuário.",
                        "details": {"reason": "all_tenants_forbidden"},
                        "hint": "Execute bootstrap apenas no tenant atual.",
                    },
                )
            rows = ensure_baseline_ruleset_for_all_tenants(
                db,
                actor=current_user.id,
                ruleset_name=body.ruleset_name,
                stale_days=body.stale_days,
                legacy_os_definitions=body.legacy_os_definitions,
            )
            return ApiResponse(
                data=[
                    ClassificationMigrationBootstrapResponse(
                        tenant_id=row["tenant_id"],
                        ruleset_id=row["ruleset_id"],
                        ruleset_name=row["ruleset_name"],
                        published_version=row["published_version"],
                        created_now=row["created_now"],
                        phase=ClassificationMigrationPhase(row["phase"]),
                    )
                    for row in rows
                ]
            )

        row = ensure_baseline_ruleset_for_tenant(
            db,
            tenant_id=current_user.tenant_id,
            actor=current_user.id,
            ruleset_name=body.ruleset_name,
            stale_days=body.stale_days,
            legacy_os_definitions=body.legacy_os_definitions,
        )
        return ApiResponse(
            data=[
                ClassificationMigrationBootstrapResponse(
                    tenant_id=current_user.tenant_id,
                    ruleset_id=row["ruleset_id"],
                    ruleset_name=row["ruleset_name"],
                    published_version=row["published_version"],
                    created_now=row["created_now"],
                    phase=ClassificationMigrationPhase(row["phase"]),
                )
            ]
        )
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.put(
    "/classification/migration/promote-phase",
    response_model=ApiResponse[ClassificationMigrationStateResponse],
)
def put_classification_migration_promote_phase(
    body: ClassificationMigrationPromoteRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    try:
        ensure_baseline_ruleset_for_tenant(
            db,
            tenant_id=current_user.tenant_id,
            actor=current_user.id,
        )
        state = promote_classification_migration_phase(
            db,
            tenant_id=current_user.tenant_id,
            target_phase=body.target_phase,
            updated_by=current_user.id,
            enforce_parity=body.enforce_parity,
        )
        return ApiResponse(data=_to_migration_state_response(state))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.get(
    "/classification/parity-report",
    response_model=ApiResponse[ShadowParityReportResponse],
)
def get_classification_parity_report_endpoint(
    dataset_version_id: str = Query(..., min_length=1),
    run_id: str | None = Query(default=None),
    limit: int = Query(default=10_000, ge=1, le=100_000),
    persist_snapshot: bool = Query(default=True),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    try:
        report = build_shadow_parity_report(
            db,
            tenant_id=current_user.tenant_id,
            dataset_version_id=dataset_version_id,
            run_id=run_id,
            limit=limit,
            persist_snapshot=persist_snapshot,
            actor=current_user.id,
        )
        return ApiResponse(data=ShadowParityReportResponse(**report))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.get(
    "/classification/migration/inventory",
    response_model=ApiResponse[list[LegacyRuleInventoryResponse]],
)
def get_classification_migration_inventory_endpoint(
    stale_days: int | None = Query(default=None, ge=1, le=3650),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    del db, current_user
    rows = get_legacy_rule_inventory(stale_days=stale_days)
    return ApiResponse(data=[LegacyRuleInventoryResponse(**row) for row in rows])


@router.get(
    "/classification/divergences",
    response_model=ApiResponse[list[ClassificationDivergenceResponse]],
)
def get_classification_divergences_endpoint(
    limit: int = Query(default=100, ge=1, le=1_000),
    dataset_version_id: str | None = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    rows = list_recent_divergences(
        db,
        tenant_id=current_user.tenant_id,
        limit=limit,
        dataset_version_id=dataset_version_id,
    )
    return ApiResponse(data=[_to_divergence_response(row) for row in rows])


@router.get(
    "/classification/metrics",
    response_model=ApiResponse[list[ClassificationRunMetricsResponse]],
)
def get_classification_metrics_endpoint(
    limit: int = Query(default=100, ge=1, le=1_000),
    include_without_classification: bool = Query(default=False),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    runs = list_recent_classification_runs(
        db,
        tenant_id=current_user.tenant_id,
        limit=limit,
    )
    items: list[ClassificationRunMetricsResponse] = []
    for run in runs:
        metrics = _extract_classification_metrics(run.metrics_json)
        if not include_without_classification and not metrics:
            continue
        items.append(_to_run_metrics_response(run))
    return ApiResponse(data=items)
