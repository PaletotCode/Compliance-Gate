"""
datasets.py — HTTP routes for dataset registry and ingest pipeline.

Endpoints:
  POST /api/v1/datasets/machines/preview   — dry-run, no persist
  POST /api/v1/datasets/machines/ingest    — full ingest, persists dataset_version
  GET  /api/v1/datasets/machines           — list versions
  GET  /api/v1/datasets/machines/{id}      — get version detail
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Path as FPath, Query
from pydantic import AliasChoices, BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session

from compliance_gate.authentication.http.dependencies import require_role
from compliance_gate.authentication.models import Role, User
from compliance_gate.Engine.config.engine_settings import engine_settings
from compliance_gate.Engine.models import EngineArtifact
from compliance_gate.domains.machines.ingest.mapping_profile import CsvTabConfig, CsvTabDraftConfig
from compliance_gate.domains.machines.ingest.pipeline import run_ingest_pipeline
from compliance_gate.domains.machines.ingest.preview import run_preview
from compliance_gate.domains.machines.ingest.sources import MACHINES_SOURCES_BY_NAME
from compliance_gate.infra.db.session import get_db
from compliance_gate.infra.storage.data_dir_resolver import resolve_data_dir
from compliance_gate.infra.storage import datasets_store as store
from compliance_gate.infra.storage import profiles_store, uploads_store

router = APIRouter(prefix="/datasets/machines", tags=["datasets"])
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Request / Response schemas
# ─────────────────────────────────────────────────────────────────────────────


class PreviewRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    data_dir: Optional[str] = None
    upload_session_id: Optional[str] = None
    profile_ids: dict[str, str] = Field(default_factory=dict)
    inline_configs: dict[str, CsvTabConfig | CsvTabDraftConfig] = Field(
        default_factory=dict,
        validation_alias=AliasChoices("inline_configs", "configs", "source_configs"),
    )
    persist_inline_configs: bool = True


class IngestRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    source: str = "path"  # "path" only for Chat 1
    data_dir: Optional[str] = None
    upload_session_id: Optional[str] = None
    # Deprecated. Tenant comes from authenticated user token.
    tenant_id: Optional[str] = None
    profile_ids: dict[str, str] = Field(default_factory=dict)
    inline_configs: dict[str, CsvTabConfig | CsvTabDraftConfig] = Field(
        default_factory=dict,
        validation_alias=AliasChoices("inline_configs", "configs", "source_configs"),
    )
    persist_inline_configs: bool = True


class DatasetVersionSchema(BaseModel):
    id: str
    tenant_id: str
    status: str
    source_type: str
    data_dir: Optional[str]
    created_at: str
    files: Optional[list[dict]] = None
    metrics: Optional[dict] = None

    class Config:
        from_attributes = True


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _load_profile_configs(
    db: Session,
    *,
    tenant_id: str,
    profile_ids: dict[str, str],
) -> dict[str, CsvTabConfig | dict]:
    configs: dict[str, CsvTabConfig | dict] = {}
    for src_name, p_id in profile_ids.items():
        normalized_source = src_name.upper().strip()
        if normalized_source not in MACHINES_SOURCES_BY_NAME:
            raise HTTPException(
                status_code=400,
                detail=f"source inválido em profile_ids: {src_name}",
            )
        profile = profiles_store.get_profile_by_id(db, p_id)
        if not profile or profile.tenant_id != tenant_id:
            log.warning(
                "datasets.profile invalid id source=%s profile_id=%s tenant_id=%s",
                normalized_source,
                p_id,
                tenant_id,
            )
            raise HTTPException(
                status_code=400, detail=f"profile_id inválido para source={normalized_source}"
            )
        payload = profiles_store.get_active_payload(db, p_id)
        if payload is None:
            log.warning(
                "datasets.profile missing payload source=%s profile_id=%s tenant_id=%s",
                normalized_source,
                p_id,
                tenant_id,
            )
            raise HTTPException(
                status_code=400,
                detail=f"profile_id sem payload ativo para source={normalized_source}",
            )
        if isinstance(payload, CsvTabConfig):
            configs[normalized_source] = payload
        elif isinstance(payload, dict):
            if not hasattr(db, "query"):
                configs[normalized_source] = payload
            else:
                try:
                    configs[normalized_source] = CsvTabConfig.model_validate(payload)
                except Exception:
                    # Backward-compatible fallback for legacy payloads.
                    configs[normalized_source] = payload
        else:
            configs[normalized_source] = payload
    return configs


def _coerce_inline_config_map(raw: object) -> dict[str, CsvTabConfig | CsvTabDraftConfig]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, CsvTabConfig | CsvTabDraftConfig] = {}
    for source_name, payload in raw.items():
        if not isinstance(source_name, str):
            continue
        try:
            if isinstance(payload, CsvTabConfig | CsvTabDraftConfig):
                out[source_name] = payload
            elif isinstance(payload, dict):
                try:
                    out[source_name] = CsvTabConfig.model_validate(payload)
                except Exception:
                    out[source_name] = CsvTabDraftConfig.model_validate(payload)
        except Exception:
            continue
    return out


def _extract_inline_configs(body: PreviewRequest | IngestRequest) -> dict[str, CsvTabConfig | CsvTabDraftConfig]:
    merged: dict[str, CsvTabConfig | CsvTabDraftConfig] = {}
    merged.update(_coerce_inline_config_map(body.inline_configs))
    extras = body.model_extra or {}
    for key in ("inline_configs", "configs", "source_configs", "config_by_source", "profiles_payload"):
        merged.update(_coerce_inline_config_map(extras.get(key)))
    return merged


def _persist_session_drafts(
    db: Session,
    *,
    session_id: str,
    inline_configs: dict[str, CsvTabConfig | CsvTabDraftConfig],
) -> None:
    if not hasattr(db, "query"):
        return
    for source_name, cfg in inline_configs.items():
        normalized_source = source_name.upper().strip()
        if normalized_source not in MACHINES_SOURCES_BY_NAME:
            continue
        draft = cfg if isinstance(cfg, CsvTabDraftConfig) else CsvTabDraftConfig.from_complete(cfg)
        uploads_store.upsert_draft_config(
            db,
            session_id=session_id,
            source=normalized_source,
            draft=draft,
        )


def _resolve_effective_configs(
    db: Session,
    *,
    tenant_id: str,
    upload_session_id: str | None,
    profile_ids: dict[str, str],
    inline_configs: dict[str, CsvTabConfig | CsvTabDraftConfig],
) -> tuple[dict[str, CsvTabConfig | dict], dict[str, dict], list[str]]:
    warnings: list[str] = []
    configs = _load_profile_configs(
        db,
        tenant_id=tenant_id,
        profile_ids=profile_ids,
    )

    if upload_session_id and hasattr(db, "query"):
        session = uploads_store.get_session_by_id(db, upload_session_id)
        if session and session.tenant_id == tenant_id:
            session_drafts = uploads_store.get_draft_configs(db, session_id=session.id)
            for source_name, draft in session_drafts.items():
                complete = draft.to_complete_config(configs.get(source_name))
                if complete is not None:
                    configs[source_name] = complete
                else:
                    warnings.append(
                        f"draft incompleto ignorado para source={source_name} (sic_column ausente)"
                    )

    for source_name, cfg in inline_configs.items():
        normalized_source = source_name.upper().strip()
        if normalized_source not in MACHINES_SOURCES_BY_NAME:
            raise HTTPException(
                status_code=400,
                detail=f"source inválido em inline_configs: {source_name}",
            )
        if isinstance(cfg, CsvTabConfig):
            configs[normalized_source] = cfg
            continue
        merged = cfg.to_complete_config(configs.get(normalized_source))
        if merged is None:
            warnings.append(
                f"inline config incompleto ignorado para source={normalized_source} (sic_column ausente)"
            )
            continue
        configs[normalized_source] = merged

    if hasattr(db, "query"):
        for source_name in MACHINES_SOURCES_BY_NAME:
            if source_name in configs:
                continue
            default_payload = profiles_store.get_default_profile_payload(
                db,
                tenant_id=tenant_id,
                source=source_name,
            )
            if default_payload:
                configs[source_name] = default_payload

    payload_snapshots: dict[str, dict] = {}
    for source_name, cfg in configs.items():
        if isinstance(cfg, CsvTabConfig):
            payload_snapshots[source_name] = cfg.model_dump(mode="json")
        elif isinstance(cfg, dict):
            payload_snapshots[source_name] = cfg
        else:
            payload_snapshots[source_name] = {"value": str(cfg)}
    return configs, payload_snapshots, warnings


def _to_runtime_configs(
    configs: dict[str, CsvTabConfig | dict],
) -> tuple[dict[str, CsvTabConfig], list[str]]:
    runtime: dict[str, CsvTabConfig] = {}
    warnings: list[str] = []
    for source_name, cfg in configs.items():
        if isinstance(cfg, CsvTabConfig):
            runtime[source_name] = cfg
            continue
        try:
            runtime[source_name] = CsvTabConfig.model_validate(cfg)
        except Exception:
            warnings.append(
                f"config inválido ignorado para source={source_name}; usando autodetecção"
            )
    return runtime, warnings


def _version_to_schema(v) -> dict:
    try:
        used_profile_ids = json.loads(v.used_profile_ids or "{}")
        if not isinstance(used_profile_ids, dict):
            used_profile_ids = {}
    except Exception:
        used_profile_ids = {}

    try:
        used_profile_payloads = json.loads(v.used_profile_payloads or "{}")
        if not isinstance(used_profile_payloads, dict):
            used_profile_payloads = {}
    except Exception:
        used_profile_payloads = {}

    files_out = []
    for f in v.files or []:
        files_out.append(
            {
                "id": f.id,
                "source": f.source,
                "original_filename": f.original_filename,
                "checksum_sha256": f.checksum_sha256,
                "file_size_bytes": f.file_size_bytes,
                "detected_encoding": f.detected_encoding,
                "detected_delimiter": f.detected_delimiter,
                "header_row_index": f.header_row_index,
                "headers": json.loads(f.detected_headers or "[]"),
                "rows_read": f.rows_read,
                "rows_valid": f.rows_valid,
                "warnings": json.loads(f.parse_warnings or "[]"),
            }
        )

    metrics_out = None
    if v.metrics:
        m = v.metrics
        metrics_out = {
            "total_entries": m.total_entries,
            "from_ad": m.from_ad,
            "from_uem": m.from_uem,
            "from_edr": m.from_edr,
            "match_ad_uem": m.match_ad_uem,
            "match_ad_edr": m.match_ad_edr,
            "match_rate": m.match_rate,
            "asset_matched": m.asset_matched,
            "cloned_serials": m.cloned_serials,
            "parse_rate": m.parse_rate,
            "rows_read_total": m.rows_read_total,
            "rows_valid_total": m.rows_valid_total,
            "total_elapsed_ms": m.total_elapsed_ms,
            "warnings_count": m.warnings_count,
        }

    return {
        "id": v.id,
        "tenant_id": v.tenant_id,
        "status": v.status,
        "source_type": v.source_type,
        "data_dir": v.data_dir,
        "used_profile_ids": used_profile_ids,
        "used_profile_payloads": used_profile_payloads,
        "created_at": v.created_at.isoformat() if v.created_at else None,
        "files": files_out,
        "metrics": metrics_out,
    }


def _validate_artifact_path(path: Path, tenant_id: str) -> Path:
    base = Path(engine_settings.artifacts_base_dir).resolve()
    tenant_base = (base / tenant_id).resolve()
    resolved = path.resolve()
    if not resolved.is_relative_to(tenant_base):
        raise HTTPException(status_code=400, detail="artifact path outside tenant scope")
    return resolved


def _cleanup_empty_dirs(path: Path, stop_at: Path) -> None:
    current = path.parent
    while current != stop_at and current != current.parent:
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent


# ─────────────────────────────────────────────────────────────────────────────
# POST /preview
# ─────────────────────────────────────────────────────────────────────────────


@router.post("/preview")
def preview_machines(
    body: PreviewRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    """
    Dry-run ingest: detects layouts, validates headers, builds master map.
    Nothing is written to the database.
    """
    data_dir = resolve_data_dir(
        db=db,
        tenant_id=current_user.tenant_id,
        data_dir=body.data_dir,
        upload_session_id=body.upload_session_id,
    )
    inline_configs = _extract_inline_configs(body)
    if body.upload_session_id and body.persist_inline_configs and inline_configs and hasattr(db, "query"):
        session = uploads_store.get_session_by_id(db, body.upload_session_id)
        if session and session.tenant_id == current_user.tenant_id:
            _persist_session_drafts(
                db,
                session_id=session.id,
                inline_configs=inline_configs,
            )
            db.commit()

    configs, _, warnings = _resolve_effective_configs(
        db,
        tenant_id=current_user.tenant_id,
        upload_session_id=body.upload_session_id,
        profile_ids=body.profile_ids,
        inline_configs=inline_configs,
    )
    if hasattr(db, "query"):
        runtime_configs, runtime_warnings = _to_runtime_configs(configs)
    else:
        runtime_configs, runtime_warnings = configs, []
    result = run_preview(data_dir, configs=runtime_configs)
    response = result.to_response()
    combined_warnings = [*warnings, *runtime_warnings]
    if combined_warnings:
        response.setdefault("warnings", [])
        response["warnings"] = [*response["warnings"], *combined_warnings]
    return response


# ─────────────────────────────────────────────────────────────────────────────
# POST /ingest
# ─────────────────────────────────────────────────────────────────────────────


@router.post("/ingest")
def ingest_machines(
    body: IngestRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    """
    Full ingest: run pipeline, persist dataset_version + files + metrics.
    Returns dataset_version_id for use in subsequent queries.
    """
    data_dir = resolve_data_dir(
        db=db,
        tenant_id=current_user.tenant_id,
        data_dir=body.data_dir,
        upload_session_id=body.upload_session_id,
    )
    inline_configs = _extract_inline_configs(body)
    if body.upload_session_id and body.persist_inline_configs and inline_configs and hasattr(db, "query"):
        session = uploads_store.get_session_by_id(db, body.upload_session_id)
        if session and session.tenant_id == current_user.tenant_id:
            _persist_session_drafts(
                db,
                session_id=session.id,
                inline_configs=inline_configs,
            )
            db.commit()

    configs, payload_snapshots, config_warnings = _resolve_effective_configs(
        db,
        tenant_id=current_user.tenant_id,
        upload_session_id=body.upload_session_id,
        profile_ids=body.profile_ids,
        inline_configs=inline_configs,
    )
    if hasattr(db, "query"):
        runtime_configs, runtime_warnings = _to_runtime_configs(configs)
    else:
        runtime_configs, runtime_warnings = configs, []
    normalized_profile_ids = {
        source.upper().strip(): profile_id
        for source, profile_id in body.profile_ids.items()
    }

    # Create pending version
    version = store.create_dataset_version(
        db,
        tenant_id=current_user.tenant_id,
        source_type="machines",
        data_dir=str(data_dir),
        profile_ids_map=normalized_profile_ids,
        profile_payloads_map=payload_snapshots,
        actor=current_user.id,
    )

    try:
        ingest_result = run_ingest_pipeline(
            data_dir,
            dataset_version_id=version.id,
            configs=runtime_configs,
        )

        # Register files
        for fi in ingest_result.files:
            r = fi.read_result
            store.register_file(
                db,
                version_id=version.id,
                source=fi.source,
                original_filename=r.path.name if r.path else None,
                resolved_path=str(r.path) if r.path else None,
                checksum_sha256=r.checksum_sha256 or None,
                file_size_bytes=r.file_size_bytes or None,
                detected_encoding=r.detected_encoding,
                detected_delimiter=r.detected_delimiter,
                header_row_index=r.header_row_index,
                detected_headers=r.detected_headers,
                rows_read=r.rows_read,
                rows_valid=r.rows_read,  # same as read for now (no row-level filtering)
                parse_warnings=r.warnings,
            )

        # Save metrics
        jm = ingest_result.metrics.join
        store.save_metrics(
            db,
            version_id=version.id,
            total_entries=jm.total_entries if jm else 0,
            from_ad=jm.from_ad if jm else 0,
            from_uem=jm.from_uem if jm else 0,
            from_edr=jm.from_edr if jm else 0,
            match_ad_uem=jm.match_ad_uem if jm else 0,
            match_ad_edr=jm.match_ad_edr if jm else 0,
            asset_matched=jm.asset_matched if jm else 0,
            cloned_serials=jm.cloned_serials if jm else 0,
            rows_read_total=ingest_result.metrics.rows_read_total,
            rows_valid_total=ingest_result.metrics.rows_valid_total,
            total_elapsed_ms=ingest_result.metrics.total_elapsed_ms,
            warnings_count=len(ingest_result.warnings),
        )

        store.finalize_dataset_version(db, version, status="success", actor=current_user.id)
        db.commit()

        return {
            "status": "success",
            "dataset_version_id": version.id,
            "total_records": len(ingest_result.records),
            "metrics": ingest_result.metrics.to_dict(),
            "file_checksums": {
                fi.source: fi.read_result.checksum_sha256
                for fi in ingest_result.files
                if fi.read_result.ok
            },
            "warnings": [*ingest_result.warnings, *config_warnings, *runtime_warnings],
        }

    except Exception as exc:
        store.finalize_dataset_version(db, version, status="failed", actor=current_user.id)
        db.commit()
        raise HTTPException(status_code=500, detail="Ingest failed") from exc


# ─────────────────────────────────────────────────────────────────────────────
# GET / (list)
# ─────────────────────────────────────────────────────────────────────────────


@router.get("")
def list_versions(
    tenant_id: Optional[str] = Query(
        None, description="Deprecated. Ignored in favor of current tenant"
    ),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    """List dataset_versions for a tenant, newest first."""
    versions, total = store.list_versions(
        db,
        tenant_id=current_user.tenant_id,
        limit=limit,
        offset=offset,
    )
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": [_version_to_schema(v) for v in versions],
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /{id}
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/{version_id}")
def get_version(
    version_id: str = FPath(..., description="dataset_version UUID"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    """Get a specific dataset_version with files and metrics."""
    v = store.get_version_by_id(db, version_id)
    if not v or v.tenant_id != current_user.tenant_id:
        raise HTTPException(status_code=404, detail=f"dataset_version {version_id} not found")
    return _version_to_schema(v)


@router.delete("/{version_id}")
def delete_version(
    version_id: str = FPath(..., description="dataset_version UUID"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    version = store.get_version_by_id(db, version_id)
    if not version or version.tenant_id != current_user.tenant_id:
        raise HTTPException(status_code=404, detail=f"dataset_version {version_id} not found")

    store.acquire_dataset_version_lock(db, version_id)
    tenant_artifacts_root = (Path(engine_settings.artifacts_base_dir).resolve() / current_user.tenant_id).resolve()
    artifacts = (
        db.query(EngineArtifact)
        .filter(
            EngineArtifact.tenant_id == current_user.tenant_id,
            EngineArtifact.dataset_version_id == version.id,
        )
        .all()
    )

    deleted_artifacts = 0
    for artifact in artifacts:
        artifact_path = _validate_artifact_path(Path(artifact.path), current_user.tenant_id)
        if artifact_path.exists() and artifact_path.is_file():
            artifact_path.unlink()
            deleted_artifacts += 1
            _cleanup_empty_dirs(artifact_path, tenant_artifacts_root)

    store.delete_dataset_version(
        db,
        version=version,
        actor=current_user.id,
        details={"deleted_artifacts": deleted_artifacts},
    )
    db.commit()
    return {"status": "ok", "message": "dataset_version deleted", "deleted_artifacts": deleted_artifacts}
