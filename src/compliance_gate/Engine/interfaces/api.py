from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any
import json

from fastapi import APIRouter, Depends, HTTPException, Query
import polars as pl
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
from compliance_gate.domains.machines.schemas import MachineItemSchema
from compliance_gate.infra.db.session import get_db
from compliance_gate.Engine.models import EngineArtifact
from compliance_gate.shared.schemas.pagination import PaginationParams
from compliance_gate.shared.schemas.responses import (
    ApiResponse,
    PaginatedResponse,
    PaginatedResult,
    PaginationMeta,
)

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


def _apply_materialized_filters(
    df: pl.DataFrame,
    *,
    search: str | None,
    pa_code: str | None,
    statuses: list[str] | None,
    flags: list[str] | None,
) -> pl.DataFrame:
    if search:
        pattern = f"(?i){re.escape(search)}"
        df = df.filter(pl.col("hostname").fill_null("").str.contains(pattern))
    if pa_code:
        df = df.filter(pl.col("pa_code") == pa_code)
    if statuses:
        df = df.filter(pl.col("primary_status").is_in(statuses))
    if flags:
        for flag in flags:
            df = df.filter(pl.col("flags").list.contains(flag).fill_null(False))
    return df


@router.get("/tables/machines", response_model=PaginatedResponse[MachineItemSchema])
def get_materialized_table(
    pagination: PaginationParams = Depends(),
    search: str | None = Query(None, description="Busca textual por hostname"),
    pa_code: str | None = Query(None, description="Filtro por PA"),
    statuses: list[str] | None = Query(None, description="Filtro por lista de statuses"),
    flags: list[str] | None = Query(None, description="Filtro por lista de flags"),
    dataset_version_id: str = Query(..., description="dataset_version id materializado"),
    tenant_id: str | None = Query(
        None, description="Optional. Must match authenticated tenant when provided."
    ),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    if tenant_id and tenant_id != current_user.tenant_id:
        raise HTTPException(status_code=403, detail="cross-tenant access is not allowed")

    artifact = (
        db.query(EngineArtifact)
        .filter(
            EngineArtifact.tenant_id == current_user.tenant_id,
            EngineArtifact.dataset_version_id == dataset_version_id,
            EngineArtifact.artifact_type == "parquet",
            EngineArtifact.artifact_name == "machines_final",
        )
        .first()
    )
    if artifact is None:
        raise HTTPException(status_code=404, detail="machines_final artifact not found; materialize first")

    artifact_path = Path(artifact.path)
    if not artifact_path.exists():
        raise HTTPException(status_code=404, detail="artifact parquet path not found on disk")

    try:
        df = pl.read_parquet(artifact_path)
    except Exception as exc:
        log.error("engine table read failed: %s", exc)
        raise HTTPException(status_code=500, detail="failed to read materialized table") from exc

    filtered_df = _apply_materialized_filters(
        df,
        search=search,
        pa_code=pa_code,
        statuses=statuses,
        flags=flags,
    )
    total = filtered_df.height
    offset = (pagination.page - 1) * pagination.size
    paginated_df = filtered_df.slice(offset, pagination.size)

    base_keys = {
        "machine_id",
        "hostname",
        "pa_code",
        "primary_status",
        "primary_status_label",
        "flags",
        "has_ad",
        "has_uem",
        "has_edr",
        "has_asset",
        "model",
        "ip",
        "tags",
        "main_user",
        "ad_os",
        "us_ad",
        "us_uem",
        "us_edr",
        "uem_extra_user_logado",
        "edr_os",
        "status_check_win11",
        "uem_serial",
        "edr_serial",
        "chassis",
        "selected_data_json",
        "selected_data",
    }

    items: list[MachineItemSchema] = []
    for row in paginated_df.to_dicts():
        selected_data: dict[str, Any] = {}
        raw_selected = row.get("selected_data_json")
        if isinstance(raw_selected, str) and raw_selected:
            try:
                parsed_selected = json.loads(raw_selected)
                if isinstance(parsed_selected, dict):
                    selected_data = parsed_selected
            except json.JSONDecodeError:
                selected_data = {}

        payload: dict[str, Any] = {
            "id": row.get("machine_id") or row.get("hostname") or "",
            "hostname": row.get("hostname") or "",
            "pa_code": row.get("pa_code") or "",
            "primary_status": row.get("primary_status") or "",
            "primary_status_label": row.get("primary_status_label") or "",
            "flags": row.get("flags") or [],
            "has_ad": bool(row.get("has_ad")),
            "has_uem": bool(row.get("has_uem")),
            "has_edr": bool(row.get("has_edr")),
            "has_asset": bool(row.get("has_asset")),
            "model": row.get("model"),
            "ip": row.get("ip"),
            "tags": row.get("tags"),
            "main_user": row.get("main_user"),
            "ad_os": row.get("ad_os"),
            "us_ad": row.get("us_ad"),
            "us_uem": row.get("us_uem"),
            "us_edr": row.get("us_edr"),
            "uem_extra_user_logado": row.get("uem_extra_user_logado"),
            "edr_os": row.get("edr_os"),
            "status_check_win11": row.get("status_check_win11"),
            "uem_serial": row.get("uem_serial"),
            "edr_serial": row.get("edr_serial"),
            "chassis": row.get("chassis"),
            "selected_data": selected_data,
        }

        # Keep every extra parquet column available for the frontend column manager.
        for key, value in row.items():
            if key not in base_keys:
                payload[key] = value
        for key, value in selected_data.items():
            payload.setdefault(key, value)

        items.append(MachineItemSchema.model_validate(payload))

    meta = PaginationMeta(
        total=total,
        page=pagination.page,
        size=pagination.size,
        has_next=(pagination.page * pagination.size) < total,
        has_previous=pagination.page > 1,
    )
    return PaginatedResponse(data=PaginatedResult(items=items, meta=meta))


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
