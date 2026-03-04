from pydantic import BaseModel, Field
from typing import Generic, TypeVar, Any, Sequence

T = TypeVar("T")

class ErrorDetail(BaseModel):
    code: str
    message: str

class ApiResponse(BaseModel, Generic[T]):
    success: bool = True
    data: T | None = None
    errors: list[ErrorDetail] | None = None

class PaginationMeta(BaseModel):
    total: int
    page: int
    size: int
    has_next: bool
    has_previous: bool

class PaginatedResult(BaseModel, Generic[T]):
    items: Sequence[T]
    meta: PaginationMeta

class PaginatedResponse(ApiResponse[PaginatedResult[T]], Generic[T]):
    pass
