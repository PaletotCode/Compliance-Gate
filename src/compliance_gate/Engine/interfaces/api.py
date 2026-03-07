from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from compliance_gate.authentication.http.dependencies import require_role
from compliance_gate.authentication.models import Role, User
from compliance_gate.Engine.materialization.materialize_machines import materialize_machines_spine
from compliance_gate.Engine.reports.definitions import ReportRequest
from compliance_gate.Engine.reports.runner import ReportRunner
from compliance_gate.Engine.validation.explain import explain_report
from compliance_gate.Engine.validation.guardrails import (
    EngineGuardrailException,
    validate_report_request,
)
from compliance_gate.infra.db.session import get_db
from compliance_gate.shared.schemas.responses import ApiResponse

log = logging.getLogger(__name__)

router = APIRouter(prefix="/engine", tags=["Engine Core"])


class MaterializeResponse(BaseModel):
    artifact_id: str
    tenant_id: str
    dataset_version_id: str
    artifact_name: str
    path: str
    checksum: str | None = None
    row_count: int


class ReportPreviewResponse(BaseModel):
    template_name: str
    query: str
    explain_plan: str
    sample: list[dict[str, Any]]


class ReportRunResponse(BaseModel):
    template_name: str
    query: str
    row_count: int
    data: list[dict[str, Any]]


@router.post("/materialize/machines", response_model=ApiResponse[MaterializeResponse])
def materialize_machines(
    dataset_version_id: str = Query(..., description="dataset_version id"),
    tenant_id: str | None = Query(
        None, description="Optional. Must match authenticated tenant when provided."
    ),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    try:
        if tenant_id and tenant_id != current_user.tenant_id:
            raise HTTPException(status_code=403, detail="cross-tenant access is not allowed")
        resolved_tenant = current_user.tenant_id
        artifact = materialize_machines_spine(db, resolved_tenant, dataset_version_id)
        return ApiResponse(
            data=MaterializeResponse(
                artifact_id=artifact.id,
                tenant_id=artifact.tenant_id,
                dataset_version_id=artifact.dataset_version_id,
                artifact_name=artifact.artifact_name,
                path=artifact.path,
                checksum=artifact.checksum,
                row_count=artifact.row_count or 0,
            )
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        log.error("engine materialize failed: %s", exc)
        raise HTTPException(status_code=500, detail="engine materialize failed") from exc


@router.post("/reports/preview", response_model=ApiResponse[ReportPreviewResponse])
def preview_report(
    body: dict[str, Any],
    dataset_version_id: str = Query(...),
    tenant_id: str | None = Query(
        None, description="Optional. Must match authenticated tenant when provided."
    ),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    try:
        validate_report_request(body)
        request = ReportRequest(**body)
        if tenant_id and tenant_id != current_user.tenant_id:
            raise HTTPException(status_code=403, detail="cross-tenant access is not allowed")
        resolved_tenant = current_user.tenant_id
        preview = explain_report(
            db,
            tenant_id=resolved_tenant,
            dataset_version_id=dataset_version_id,
            request=request,
        )
        return ApiResponse(
            data=ReportPreviewResponse(
                template_name=request.template_name,
                query=preview["query"],
                explain_plan=preview["explain_plan"],
                sample=preview["sample"],
            )
        )
    except EngineGuardrailException as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        log.error("engine report preview failed: %s", exc)
        raise HTTPException(status_code=500, detail="engine report preview failed") from exc


@router.post("/reports/run", response_model=ApiResponse[ReportRunResponse])
def run_report(
    body: dict[str, Any],
    dataset_version_id: str = Query(...),
    tenant_id: str | None = Query(
        None, description="Optional. Must match authenticated tenant when provided."
    ),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    try:
        validate_report_request(body)
        request = ReportRequest(**body)
        if tenant_id and tenant_id != current_user.tenant_id:
            raise HTTPException(status_code=403, detail="cross-tenant access is not allowed")
        resolved_tenant = current_user.tenant_id
        rows, resolved_plan = ReportRunner.execute(
            db,
            tenant_id=resolved_tenant,
            dataset_version_id=dataset_version_id,
            request=request,
        )
        return ApiResponse(
            data=ReportRunResponse(
                template_name=resolved_plan.template_name,
                query=resolved_plan.query,
                row_count=len(rows),
                data=rows,
            )
        )
    except EngineGuardrailException as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except TimeoutError as exc:
        raise HTTPException(status_code=408, detail=str(exc)) from exc
    except Exception as exc:
        log.error("engine report run failed: %s", exc)
        raise HTTPException(status_code=500, detail="engine report run failed") from exc
