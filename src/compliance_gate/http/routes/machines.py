from fastapi import APIRouter, Depends, HTTPException, Path, Query
from typing import List, Optional
from compliance_gate.authentication.http.dependencies import require_role
from compliance_gate.authentication.models import Role, User
from compliance_gate.shared.schemas.responses import ApiResponse, PaginatedResponse, PaginatedResult, PaginationMeta
from compliance_gate.shared.schemas.pagination import PaginationParams
from compliance_gate.domains.machines.schemas import (
    MachineFilterSchema,
    MachineItemSchema,
    MachineSummarySchema,
    FilterDefinitionSchema
)
from compliance_gate.domains.machines.service import MachinesService
from compliance_gate.infra.logging import debug_logger
from compliance_gate.infra.db.session import get_db
from sqlalchemy.orm import Session

router = APIRouter(prefix="/machines", tags=["machines"])

_NO_DATASET_MSG = (
    "Nenhum dataset_version encontrado. "
    "Execute POST /api/v1/datasets/machines/ingest primeiro."
)


@router.get("/filters", response_model=ApiResponse[List[FilterDefinitionSchema]])
def get_filters(_: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR))):
    """Returns the list of available isolated filters with their metadata."""
    filters = MachinesService.get_available_filters()
    return ApiResponse(data=filters)


@router.get("/table", response_model=PaginatedResponse[MachineItemSchema])
def get_table(
    pagination: PaginationParams = Depends(),
    search: Optional[str] = Query(None, description="Busca textual por hostname"),
    pa_code: Optional[str] = Query(None, description="Filtro por PA"),
    statuses: Optional[List[str]] = Query(None, description="Filtro por lista de Primary Statuses"),
    flags: Optional[List[str]] = Query(None, description="Filtro por lista de Flags"),
    dataset_version_id: Optional[str] = Query(None, description="UUID do dataset_version a usar (default: latest)"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    """Returns paginated table of machines matching filters."""
    filters = MachineFilterSchema(search=search, pa_code=pa_code, statuses=statuses, flags=flags)

    try:
        items, total = MachinesService.get_table_data(
            db, filters, pagination.page, pagination.size,
            tenant_id=current_user.tenant_id,
            dataset_version_id=dataset_version_id,
        )
    except MachinesService.NoDatasetError:
        raise HTTPException(status_code=422, detail=_NO_DATASET_MSG)

    meta = PaginationMeta(
        total=total,
        page=pagination.page,
        size=pagination.size,
        has_next=(pagination.page * pagination.size) < total,
        has_previous=pagination.page > 1
    )
    return PaginatedResponse(data=PaginatedResult(items=items, meta=meta))


@router.get("/summary", response_model=ApiResponse[MachineSummarySchema])
def get_summary(
    search: Optional[str] = Query(None),
    pa_code: Optional[str] = Query(None),
    statuses: Optional[List[str]] = Query(None),
    flags: Optional[List[str]] = Query(None),
    dataset_version_id: Optional[str] = Query(None, description="UUID do dataset_version (default: latest)"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    """Returns counters grouped by status and flags based on current filters."""
    filters = MachineFilterSchema(search=search, pa_code=pa_code, statuses=statuses, flags=flags)

    try:
        data = MachinesService.get_summary_data(
            db,
            filters,
            tenant_id=current_user.tenant_id,
            dataset_version_id=dataset_version_id,
        )
    except MachinesService.NoDatasetError:
        raise HTTPException(status_code=422, detail=_NO_DATASET_MSG)

    return ApiResponse(data=data)


@router.get("/timeline", response_model=ApiResponse[List[dict]])
def get_timeline(_: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR))):
    return ApiResponse(data=[])


@router.get("/items/{id}/history", response_model=ApiResponse[List[dict]])
def get_item_history(
    id: str = Path(...),
    _: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    return ApiResponse(data=[])


@router.get("/debug/logs")
def get_debug_logs(
    limit: int = Query(200, ge=1, le=2000),
    _: User = Depends(require_role(Role.TI_ADMIN)),
):
    """DEV-only: returns internal metrics logs from processing CSVs / master map logic."""
    logs = debug_logger.get_logs(limit)
    return {"status": 200, "data": logs}


@router.get("/debug/sample")
def get_debug_sample(
    limit: int = Query(50, ge=1, le=500),
    _: User = Depends(require_role(Role.TI_ADMIN)),
):
    """DEV-only: returns samples of constructed machine records from engine build."""
    samples = debug_logger.get_samples(limit)
    return {"status": 200, "data": samples}
