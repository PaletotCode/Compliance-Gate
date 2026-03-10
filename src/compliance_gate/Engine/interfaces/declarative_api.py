from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from compliance_gate.authentication.http.dependencies import require_role
from compliance_gate.authentication.models import Role, User
from compliance_gate.Engine.catalog.machines_final import get_machines_final_catalog
from compliance_gate.Engine.catalog.schemas import MachinesFinalCatalogSnapshot
from compliance_gate.Engine.config.engine_settings import engine_settings
from compliance_gate.Engine.declarative import (
    SegmentRecord,
    TransformationRecord,
    ViewRecord,
    create_segment,
    create_transformation,
    create_view,
    get_segment,
    get_transformation,
    get_view,
    list_segments,
    list_transformations,
    list_views,
    update_segment,
    update_transformation,
    update_view,
)
from compliance_gate.Engine.errors import DeclarativeEngineError
from compliance_gate.Engine.expressions import ExpressionNode
from compliance_gate.Engine.runtime import (
    SegmentPreviewResult,
    ViewPreviewResult,
    ViewRunResult,
    preview_segment,
    preview_view,
    run_view,
)
from compliance_gate.Engine.segments import SegmentPayloadV1
from compliance_gate.Engine.segments.templates import (
    SegmentTemplate,
    get_segment_template,
    list_segment_templates,
)
from compliance_gate.Engine.transformations import (
    TransformationOutputType,
    TransformationPayloadV1,
)
from compliance_gate.Engine.views import ViewPayloadV1, ViewSortSpec
from compliance_gate.infra.db.session import get_db
from compliance_gate.shared.schemas.responses import ApiResponse

router = APIRouter(prefix="/engine", tags=["Engine Declarative"])


class TemplateInfo(BaseModel):
    key: str
    name: str
    description: str


class SegmentFromTemplateRequest(BaseModel):
    template_key: str
    name: str
    description: str | None = None


class TransformationCreateRequest(BaseModel):
    name: str
    description: str | None = None
    output_column_name: str
    expression: ExpressionNode
    output_type: TransformationOutputType


class TransformationUpdateRequest(BaseModel):
    name: str | None = None
    description: str | None = None
    output_column_name: str | None = None
    expression: ExpressionNode | None = None
    output_type: TransformationOutputType | None = None


class SegmentCreateRequest(BaseModel):
    name: str
    description: str | None = None
    filter_expression: ExpressionNode


class SegmentUpdateRequest(BaseModel):
    name: str | None = None
    description: str | None = None
    filter_expression: ExpressionNode | None = None


class ViewCreateRequest(BaseModel):
    name: str
    description: str | None = None
    payload: ViewPayloadV1


class ViewUpdateRequest(BaseModel):
    name: str | None = None
    description: str | None = None
    payload: ViewPayloadV1 | None = None


class SegmentPreviewRequest(BaseModel):
    segment_id: str | None = None
    expression: ExpressionNode | None = None
    limit: int | None = Field(default=None, ge=1)


class ViewPreviewRequest(BaseModel):
    view_id: str | None = None
    inline_view_payload: ViewPayloadV1 | None = None
    limit: int | None = Field(default=None, ge=1)


class ViewRunRequest(BaseModel):
    view_id: str
    page: int = Field(default=1, ge=1)
    size: int = Field(default=50, ge=1, le=2_000)
    search: str | None = None
    sort: ViewSortSpec | None = None


class TransformationResponse(BaseModel):
    id: str
    tenant_id: str
    name: str
    description: str | None = None
    created_by: str | None = None
    created_at: datetime
    active_version: int
    payload: TransformationPayloadV1


class SegmentResponse(BaseModel):
    id: str
    tenant_id: str
    name: str
    description: str | None = None
    created_by: str | None = None
    created_at: datetime
    active_version: int
    payload: SegmentPayloadV1


class ViewResponse(BaseModel):
    id: str
    tenant_id: str
    name: str
    description: str | None = None
    created_by: str | None = None
    created_at: datetime
    active_version: int
    payload: ViewPayloadV1


def _err_status(exc: DeclarativeEngineError) -> int:
    reason = exc.details.get("reason")
    if isinstance(reason, str) and reason.endswith("_not_found"):
        return 404
    if reason in {"artifact_missing_on_disk"}:
        return 404
    return 400


def _raise_declarative(exc: DeclarativeEngineError) -> None:
    raise HTTPException(status_code=_err_status(exc), detail=exc.to_dict()) from exc


def _to_transformation_response(record: TransformationRecord) -> TransformationResponse:
    definition = record.definition
    return TransformationResponse(
        id=definition.id,
        tenant_id=definition.tenant_id,
        name=definition.name,
        description=definition.description,
        created_by=definition.created_by,
        created_at=definition.created_at,
        active_version=definition.active_version,
        payload=record.payload,
    )


def _to_segment_response(record: SegmentRecord) -> SegmentResponse:
    definition = record.definition
    return SegmentResponse(
        id=definition.id,
        tenant_id=definition.tenant_id,
        name=definition.name,
        description=definition.description,
        created_by=definition.created_by,
        created_at=definition.created_at,
        active_version=definition.active_version,
        payload=record.payload,
    )


def _to_view_response(record: ViewRecord) -> ViewResponse:
    definition = record.definition
    return ViewResponse(
        id=definition.id,
        tenant_id=definition.tenant_id,
        name=definition.name,
        description=definition.description,
        created_by=definition.created_by,
        created_at=definition.created_at,
        active_version=definition.active_version,
        payload=record.payload,
    )


def _fallback_transformation_payload(
    current: TransformationPayloadV1,
    update: TransformationUpdateRequest,
) -> TransformationPayloadV1:
    output_type = update.output_type or current.output_type
    return TransformationPayloadV1(
        output_column_name=update.output_column_name or current.output_column_name,
        expression=update.expression or current.expression,
        output_type=output_type,
    )


def _fallback_segment_payload(current: SegmentPayloadV1, update: SegmentUpdateRequest) -> SegmentPayloadV1:
    return SegmentPayloadV1(filter_expression=update.filter_expression or current.filter_expression)


@router.get(
    "/catalog/machines",
    response_model=ApiResponse[MachinesFinalCatalogSnapshot],
)
def get_catalog(
    dataset_version_id: str = Query(..., description="dataset_version id materializado"),
    sample_size: int = Query(5, ge=1, le=20),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    try:
        snapshot = get_machines_final_catalog(
            db,
            tenant_id=current_user.tenant_id,
            dataset_version_id=dataset_version_id,
            sample_size=sample_size,
        )
        return ApiResponse(data=snapshot)
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.get("/segments/templates", response_model=ApiResponse[list[TemplateInfo]])
def get_segment_templates(
    _: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    templates = list_segment_templates()
    return ApiResponse(
        data=[
            TemplateInfo(key=template.key, name=template.name, description=template.description)
            for template in templates
        ]
    )


@router.post("/segments/from-template", response_model=ApiResponse[SegmentResponse])
def create_segment_from_template(
    body: SegmentFromTemplateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    template: SegmentTemplate | None = get_segment_template(body.template_key)
    if template is None:
        raise HTTPException(status_code=404, detail="segment template not found")
    try:
        payload = SegmentPayloadV1(filter_expression=template.expression)
        record = create_segment(
            db,
            tenant_id=current_user.tenant_id,
            name=body.name,
            description=body.description or template.description,
            created_by=current_user.id,
            payload=payload,
        )
        return ApiResponse(data=_to_segment_response(record))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.post("/transformations", response_model=ApiResponse[TransformationResponse])
def post_transformation(
    body: TransformationCreateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    try:
        payload = TransformationPayloadV1(
            output_column_name=body.output_column_name,
            expression=body.expression,
            output_type=body.output_type,
        )
        record = create_transformation(
            db,
            tenant_id=current_user.tenant_id,
            name=body.name,
            description=body.description,
            created_by=current_user.id,
            payload=payload,
        )
        return ApiResponse(data=_to_transformation_response(record))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.put("/transformations/{transformation_id}", response_model=ApiResponse[TransformationResponse])
def put_transformation(
    transformation_id: str,
    body: TransformationUpdateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    try:
        current = get_transformation(
            db, tenant_id=current_user.tenant_id, transformation_id=transformation_id
        )
        payload = _fallback_transformation_payload(current.payload, body)
        record = update_transformation(
            db,
            tenant_id=current_user.tenant_id,
            transformation_id=transformation_id,
            name=body.name,
            description=body.description,
            created_by=current_user.id,
            payload=payload,
        )
        return ApiResponse(data=_to_transformation_response(record))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.get("/transformations", response_model=ApiResponse[list[TransformationResponse]])
def get_transformations(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    try:
        records = list_transformations(db, tenant_id=current_user.tenant_id)
        return ApiResponse(data=[_to_transformation_response(record) for record in records])
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.get("/transformations/{transformation_id}", response_model=ApiResponse[TransformationResponse])
def get_transformation_by_id(
    transformation_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    try:
        record = get_transformation(
            db, tenant_id=current_user.tenant_id, transformation_id=transformation_id
        )
        return ApiResponse(data=_to_transformation_response(record))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.post("/segments", response_model=ApiResponse[SegmentResponse])
def post_segment(
    body: SegmentCreateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    try:
        payload = SegmentPayloadV1(filter_expression=body.filter_expression)
        record = create_segment(
            db,
            tenant_id=current_user.tenant_id,
            name=body.name,
            description=body.description,
            created_by=current_user.id,
            payload=payload,
        )
        return ApiResponse(data=_to_segment_response(record))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.put("/segments/{segment_id}", response_model=ApiResponse[SegmentResponse])
def put_segment(
    segment_id: str,
    body: SegmentUpdateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    try:
        current = get_segment(db, tenant_id=current_user.tenant_id, segment_id=segment_id)
        payload = _fallback_segment_payload(current.payload, body)
        record = update_segment(
            db,
            tenant_id=current_user.tenant_id,
            segment_id=segment_id,
            name=body.name,
            description=body.description,
            created_by=current_user.id,
            payload=payload,
        )
        return ApiResponse(data=_to_segment_response(record))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.get("/segments", response_model=ApiResponse[list[SegmentResponse]])
def get_segments(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    try:
        records = list_segments(db, tenant_id=current_user.tenant_id)
        return ApiResponse(data=[_to_segment_response(record) for record in records])
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.get("/segments/{segment_id}", response_model=ApiResponse[SegmentResponse])
def get_segment_by_id(
    segment_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    try:
        record = get_segment(db, tenant_id=current_user.tenant_id, segment_id=segment_id)
        return ApiResponse(data=_to_segment_response(record))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.post("/segments/preview", response_model=ApiResponse[SegmentPreviewResult])
def post_segment_preview(
    body: SegmentPreviewRequest,
    dataset_version_id: str = Query(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    try:
        result = preview_segment(
            db,
            tenant_id=current_user.tenant_id,
            dataset_version_id=dataset_version_id,
            segment_id=body.segment_id,
            expression=body.expression,
            limit=body.limit,
        )
        return ApiResponse(data=result)
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)
    except TimeoutError as exc:
        raise HTTPException(status_code=408, detail=str(exc)) from exc


@router.post("/views", response_model=ApiResponse[ViewResponse])
def post_view(
    body: ViewCreateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    try:
        body.payload.validate_guardrails(max_row_limit=engine_settings.max_report_rows)
        record = create_view(
            db,
            tenant_id=current_user.tenant_id,
            name=body.name,
            description=body.description,
            created_by=current_user.id,
            payload=body.payload,
        )
        return ApiResponse(data=_to_view_response(record))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.put("/views/{view_id}", response_model=ApiResponse[ViewResponse])
def put_view(
    view_id: str,
    body: ViewUpdateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    try:
        current = get_view(db, tenant_id=current_user.tenant_id, view_id=view_id)
        payload = body.payload or current.payload
        payload.validate_guardrails(max_row_limit=engine_settings.max_report_rows)
        record = update_view(
            db,
            tenant_id=current_user.tenant_id,
            view_id=view_id,
            name=body.name,
            description=body.description,
            created_by=current_user.id,
            payload=payload,
        )
        return ApiResponse(data=_to_view_response(record))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.get("/views", response_model=ApiResponse[list[ViewResponse]])
def get_views(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    try:
        records = list_views(db, tenant_id=current_user.tenant_id)
        return ApiResponse(data=[_to_view_response(record) for record in records])
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.get("/views/{view_id}", response_model=ApiResponse[ViewResponse])
def get_view_by_id(
    view_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    try:
        record = get_view(db, tenant_id=current_user.tenant_id, view_id=view_id)
        return ApiResponse(data=_to_view_response(record))
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)


@router.post("/views/preview", response_model=ApiResponse[ViewPreviewResult])
def post_view_preview(
    body: ViewPreviewRequest,
    dataset_version_id: str = Query(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    try:
        result = preview_view(
            db,
            tenant_id=current_user.tenant_id,
            dataset_version_id=dataset_version_id,
            view_id=body.view_id,
            inline_view_payload=body.inline_view_payload,
            limit=body.limit,
        )
        return ApiResponse(data=result)
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)
    except TimeoutError as exc:
        raise HTTPException(status_code=408, detail=str(exc)) from exc


@router.post("/views/run", response_model=ApiResponse[ViewRunResult])
def post_view_run(
    body: ViewRunRequest,
    dataset_version_id: str = Query(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    try:
        result = run_view(
            db,
            tenant_id=current_user.tenant_id,
            dataset_version_id=dataset_version_id,
            view_id=body.view_id,
            page=body.page,
            size=body.size,
            search=body.search,
            sort_override=body.sort,
        )
        return ApiResponse(data=result)
    except DeclarativeEngineError as exc:
        _raise_declarative(exc)
    except TimeoutError as exc:
        raise HTTPException(status_code=408, detail=str(exc)) from exc
