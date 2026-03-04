from fastapi import APIRouter, Depends, Path
from typing import List
from compliance_gate.http.dependencies import PaginationDep
from compliance_gate.shared.schemas.responses import ApiResponse, PaginatedResponse, PaginatedResult, PaginationMeta
from compliance_gate.shared.schemas.pagination import PaginationParams
from compliance_gate.domains.telefonia.schemas import (
    TelefoniaFilterSchema,
    TelefoniaItemSchema,
    TelefoniaSummarySchema
)

router = APIRouter(prefix="/telefonia", tags=["telefonia"])

@router.get("/filters", response_model=ApiResponse[List[str]])
def get_filters():
    # Stub
    return ApiResponse(data=["compliant", "inconsistency"])

@router.get("/table", response_model=PaginatedResponse[TelefoniaItemSchema])
def get_table(
    pagination: PaginationParams = Depends(),
    filters: TelefoniaFilterSchema = Depends()
):
    # Stub
    meta = PaginationMeta(total=0, page=pagination.page, size=pagination.size, has_next=False, has_previous=False)
    return PaginatedResponse(data=PaginatedResult(items=[], meta=meta))

@router.get("/summary", response_model=ApiResponse[TelefoniaSummarySchema])
def get_summary(filters: TelefoniaFilterSchema = Depends()):
    # Stub
    data = TelefoniaSummarySchema(total=0, compliant=0, inconsistency=0)
    return ApiResponse(data=data)

@router.get("/timeline", response_model=ApiResponse[List[dict]])
def get_timeline():
    # Stub
    return ApiResponse(data=[])

@router.get("/items/{id}/history", response_model=ApiResponse[List[dict]])
def get_item_history(id: str = Path(...)):
    # Stub
    return ApiResponse(data=[])
