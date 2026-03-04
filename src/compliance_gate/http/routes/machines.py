from fastapi import APIRouter, Depends, Path, Query
from typing import List, Optional
from compliance_gate.http.dependencies import PaginationDep
from compliance_gate.shared.schemas.responses import ApiResponse, PaginatedResponse, PaginatedResult, PaginationMeta
from compliance_gate.shared.schemas.pagination import PaginationParams
from compliance_gate.domains.machines.schemas import (
    MachineFilterSchema,
    MachineItemSchema,
    MachineSummarySchema,
    FilterDefinitionSchema
)
from compliance_gate.domains.machines.service import MachinesService

router = APIRouter(prefix="/machines", tags=["machines"])

@router.get("/filters", response_model=ApiResponse[List[FilterDefinitionSchema]])
def get_filters():
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
):
    """Returns paginated table of machines matching filters."""
    filters = MachineFilterSchema(search=search, pa_code=pa_code, statuses=statuses, flags=flags)
    
    items, total = MachinesService.get_table_data(filters, pagination.page, pagination.size)
    
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
):
    """Returns counters grouped by status and flags based on current filters."""
    filters = MachineFilterSchema(search=search, pa_code=pa_code, statuses=statuses, flags=flags)
    data = MachinesService.get_summary_data(filters)
    return ApiResponse(data=data)

@router.get("/timeline", response_model=ApiResponse[List[dict]])
def get_timeline():
    # Stub: timeline events
    return ApiResponse(data=[])

@router.get("/items/{id}/history", response_model=ApiResponse[List[dict]])
def get_item_history(id: str = Path(...)):
    # Stub: empty history
    return ApiResponse(data=[])
