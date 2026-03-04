from fastapi import APIRouter
from compliance_gate.shared.schemas.responses import ApiResponse
from compliance_gate.config.settings import settings

router = APIRouter(tags=["health"])

@router.get("/health", response_model=ApiResponse[str])
def health_check():
    return ApiResponse(data="OK")

@router.get("/ready", response_model=ApiResponse[str])
def ready_check():
    return ApiResponse(data="Ready")

@router.get("/version", response_model=ApiResponse[str])
def version_check():
    return ApiResponse(data=settings.app_version)
