from fastapi import APIRouter, Depends, Path
from typing import List
from compliance_gate.http.dependencies import PaginationDep
from compliance_gate.shared.schemas.responses import ApiResponse, PaginatedResponse, PaginatedResult, PaginationMeta
from compliance_gate.shared.schemas.pagination import PaginationParams
from compliance_gate.domains.impressoras.schemas import (
    ImpressorasFilterSchema,
    ImpressorasItemSchema,
    ImpressorasSummarySchema
)

router = APIRouter(prefix="/impressoras", tags=["impressoras"])

@router.get("/filters", response_model=ApiResponse[List[str]])
def get_filters():
    # Stub
    return ApiResponse(data=["compliant", "offline"])

@router.get("/table", response_model=PaginatedResponse[ImpressorasItemSchema])
def get_table(
    pagination: PaginationParams = Depends(),
    filters: ImpressorasFilterSchema = Depends()
):
    # Stub
    meta = PaginationMeta(total=0, page=pagination.page, size=pagination.size, has_next=False, has_previous=False)
    return PaginatedResponse(data=PaginatedResult(items=[], meta=meta))

@router.get("/summary", response_model=ApiResponse[ImpressorasSummarySchema])
def get_summary(filters: ImpressorasFilterSchema = Depends()):
    # Stub
    data = ImpressorasSummarySchema(total=0, compliant=0, offline=0)
    return ApiResponse(data=data)

@router.get("/timeline", response_model=ApiResponse[List[dict]])
def get_timeline():
    # Stub
    return ApiResponse(data=[])

@router.get("/items/{id}/history", response_model=ApiResponse[List[dict]])
def get_item_history(id: str = Path(...)):
    # Stub
    return ApiResponse(data=[])
