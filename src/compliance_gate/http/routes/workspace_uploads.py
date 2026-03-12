from __future__ import annotations

import hashlib
import logging
import shutil
import json
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from pydantic import BaseModel
from sqlalchemy.orm import Session

from compliance_gate.authentication.http.dependencies import require_role
from compliance_gate.authentication.models import Role, User
from compliance_gate.config.settings import settings
from compliance_gate.domains.machines.ingest.mapping_profile import CsvTabDraftConfig
from compliance_gate.domains.machines.ingest.sources import MACHINES_SOURCES_BY_NAME
from compliance_gate.infra.db.session import get_db
from compliance_gate.infra.storage import uploads_store
from compliance_gate.shared.schemas.responses import ApiResponse
from compliance_gate.shared.utils.hashing import generate_hash

router = APIRouter(prefix="/workspace/uploads", tags=["workspace"])
log = logging.getLogger(__name__)

_UPLOAD_CHUNK_SIZE = 1024 * 1024
_ALLOWED_SUFFIXES = {".csv"}
_ENCODING_PROBES = ("utf-8-sig", "utf-8", "latin-1", "cp1252")


class UploadFileSchema(BaseModel):
    source: str
    original_filename: str
    stored_filename: str
    file_size_bytes: int
    checksum_sha256: str
    detected_encoding: str
    draft_config: dict | None = None
    draft_profile_id: str | None = None
    draft_profile_version: int | None = None


class UploadSessionSchema(BaseModel):
    id: str
    tenant_id: str
    status: str
    root_path: str
    total_files: int
    total_bytes: int
    created_at: str
    files: list[UploadFileSchema]


class UploadSessionCreateResponse(BaseModel):
    upload_session_id: str
    tenant_id: str
    status: str
    files: list[UploadFileSchema]


class UploadSessionListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[UploadSessionSchema]


def _detect_encoding(path: Path) -> str:
    head = path.read_bytes()[:8192]
    for enc in _ENCODING_PROBES:
        try:
            head.decode(enc)
            return enc
        except Exception:
            continue
    return "unknown"


def _ensure_under_base(path: Path, base_dir: Path) -> None:
    if not path.is_relative_to(base_dir):
        raise HTTPException(status_code=400, detail="invalid upload session path")


def _cleanup_session_dir(path: Path) -> None:
    try:
        if path.exists():
            shutil.rmtree(path)
    except Exception as exc:
        log.warning("workspace.upload cleanup failed dir_hash=%s err=%s", generate_hash(str(path))[:16], exc)


def _save_uploaded_file(upload: UploadFile, target: Path, max_bytes: int) -> tuple[str, int]:
    hasher = hashlib.sha256()
    total = 0
    with target.open("wb") as fout:
        while True:
            chunk = upload.file.read(_UPLOAD_CHUNK_SIZE)
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise HTTPException(
                    status_code=413,
                    detail=f"file too large: {upload.filename}",
                )
            hasher.update(chunk)
            fout.write(chunk)

    if total <= 0:
        raise HTTPException(status_code=400, detail=f"empty file: {upload.filename}")
    return hasher.hexdigest(), total


def _to_file_schema(row) -> UploadFileSchema:
    draft_config = None
    if getattr(row, "draft_config_json", None):
        try:
            draft_config = json.loads(row.draft_config_json)
        except Exception:
            draft_config = None
    return UploadFileSchema(
        source=row.source,
        original_filename=row.original_filename,
        stored_filename=row.stored_filename,
        file_size_bytes=row.file_size_bytes or 0,
        checksum_sha256=row.checksum_sha256 or "",
        detected_encoding=row.detected_encoding or "unknown",
        draft_config=draft_config,
        draft_profile_id=getattr(row, "draft_profile_id", None),
        draft_profile_version=getattr(row, "draft_profile_version", None),
    )


def _to_session_schema(row) -> UploadSessionSchema:
    return UploadSessionSchema(
        id=row.id,
        tenant_id=row.tenant_id,
        status=row.status,
        root_path=row.root_path,
        total_files=row.total_files or 0,
        total_bytes=row.total_bytes or 0,
        created_at=row.created_at.isoformat() if row.created_at else "",
        files=[_to_file_schema(f) for f in row.files or []],
    )


def _resolve_source_uploads(
    *,
    AD: UploadFile | None,
    UEM: UploadFile | None,
    EDR: UploadFile | None,
    ASSET: UploadFile | None,
    ad: UploadFile | None,
    uem: UploadFile | None,
    edr: UploadFile | None,
    asset: UploadFile | None,
) -> dict[str, UploadFile]:
    out: dict[str, UploadFile] = {}
    candidates = {
        "AD": (AD, ad),
        "UEM": (UEM, uem),
        "EDR": (EDR, edr),
        "ASSET": (ASSET, asset),
    }
    for source, (upper, lower) in candidates.items():
        if upper is not None and lower is not None:
            raise HTTPException(
                status_code=400,
                detail=f"duplicated upload field for source={source}",
            )
        chosen = upper or lower
        if chosen is not None:
            out[source] = chosen
    return out


@router.post("", response_model=ApiResponse[UploadSessionCreateResponse])
def create_upload_session(
    AD: UploadFile | None = File(None),
    UEM: UploadFile | None = File(None),
    EDR: UploadFile | None = File(None),
    ASSET: UploadFile | None = File(None),
    ad: UploadFile | None = File(None),
    uem: UploadFile | None = File(None),
    edr: UploadFile | None = File(None),
    asset: UploadFile | None = File(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    source_files = _resolve_source_uploads(
        AD=AD,
        UEM=UEM,
        EDR=EDR,
        ASSET=ASSET,
        ad=ad,
        uem=uem,
        edr=edr,
        asset=asset,
    )
    if not source_files:
        raise HTTPException(status_code=400, detail="at least one source file is required")

    base_dir = Path(settings.cg_upload_dir).resolve()
    max_bytes = settings.cg_upload_max_file_mb * 1024 * 1024
    session = uploads_store.create_upload_session(
        db,
        tenant_id=current_user.tenant_id,
        created_by=current_user.id,
        root_path="",  # updated right after ID generation
    )
    session_dir = (base_dir / current_user.tenant_id / session.id).resolve()
    _ensure_under_base(session_dir, base_dir)
    session.root_path = str(session_dir)

    accepted_files = []
    source_manifest: dict[str, str] = {}
    total_bytes = 0
    try:
        session_dir.mkdir(parents=True, exist_ok=True)
        for source, upload in source_files.items():
            suffix = Path(upload.filename or "").suffix.lower()
            if suffix not in _ALLOWED_SUFFIXES:
                raise HTTPException(
                    status_code=400,
                    detail=f"invalid file extension for source={source}",
                )

            source_def = MACHINES_SOURCES_BY_NAME[source]
            stored_name = source_def.filename_candidates[0]
            target_path = session_dir / stored_name

            checksum, file_size = _save_uploaded_file(upload, target_path, max_bytes=max_bytes)
            encoding = _detect_encoding(target_path)
            source_manifest[source] = stored_name
            total_bytes += file_size

            file_row = uploads_store.register_upload_file(
                db,
                session_id=session.id,
                source=source,
                original_filename=upload.filename or stored_name,
                stored_filename=stored_name,
                checksum_sha256=checksum,
                file_size_bytes=file_size,
                detected_encoding=encoding,
            )
            accepted_files.append(file_row)

            log.info(
                "workspace.upload accepted tenant_hash=%s session_hash=%s source=%s size=%d",
                generate_hash(current_user.tenant_id)[:16],
                generate_hash(session.id)[:16],
                source,
                file_size,
            )

        uploads_store.finalize_upload_session(
            db,
            session,
            total_files=len(accepted_files),
            total_bytes=total_bytes,
            source_manifest=source_manifest,
        )
        uploads_store.append_audit(
            db,
            tenant_id=current_user.tenant_id,
            actor=current_user.id,
            action="UPLOAD_SESSION_CREATE",
            entity_id=session.id,
            details={"sources": sorted(source_manifest.keys()), "total_files": len(accepted_files)},
        )
        db.commit()
    except HTTPException:
        db.rollback()
        _cleanup_session_dir(session_dir)
        raise
    except Exception as exc:
        db.rollback()
        _cleanup_session_dir(session_dir)
        log.error(
            "workspace.upload failed tenant_hash=%s session_hash=%s err=%s",
            generate_hash(current_user.tenant_id)[:16],
            generate_hash(session.id)[:16],
            exc,
        )
        raise HTTPException(status_code=500, detail="upload session failed") from exc
    finally:
        for upload in source_files.values():
            try:
                upload.file.close()
            except Exception:
                continue

    return ApiResponse(
        data=UploadSessionCreateResponse(
            upload_session_id=session.id,
            tenant_id=session.tenant_id,
            status=session.status,
            files=[_to_file_schema(f) for f in accepted_files],
        )
    )


@router.get("/sessions", response_model=ApiResponse[UploadSessionListResponse])
def list_upload_sessions(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    items, total = uploads_store.list_sessions(
        db,
        tenant_id=current_user.tenant_id,
        limit=limit,
        offset=offset,
    )
    return ApiResponse(
        data=UploadSessionListResponse(
            total=total,
            limit=limit,
            offset=offset,
            items=[_to_session_schema(s) for s in items],
        )
    )


@router.get("/sessions/{session_id}", response_model=ApiResponse[UploadSessionSchema])
def get_upload_session(
    session_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    session = uploads_store.get_session_by_id(db, session_id)
    if not session or session.tenant_id != current_user.tenant_id:
        raise HTTPException(status_code=404, detail="upload session not found")
    return ApiResponse(data=_to_session_schema(session))


@router.delete("/sessions/{session_id}", response_model=ApiResponse[str])
def delete_upload_session(
    session_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    session = uploads_store.get_session_by_id(db, session_id)
    if not session or session.tenant_id != current_user.tenant_id:
        raise HTTPException(status_code=404, detail="upload session not found")

    uploads_store.acquire_session_advisory_lock(db, session.id)

    base_dir = Path(settings.cg_upload_dir).resolve()
    session_dir = Path(session.root_path).resolve()
    _ensure_under_base(session_dir, base_dir)
    _cleanup_session_dir(session_dir)

    uploads_store.append_audit(
        db,
        tenant_id=current_user.tenant_id,
        actor=current_user.id,
        action="UPLOAD_SESSION_DELETE",
        entity_id=session.id,
        details={"total_files": session.total_files, "total_bytes": session.total_bytes},
    )
    uploads_store.delete_session(db, session)
    db.commit()
    return ApiResponse(data="deleted")


class UploadDraftConfigRequest(BaseModel):
    payload: CsvTabDraftConfig
    profile_id: str | None = None
    profile_version: int | None = None


@router.put(
    "/sessions/{session_id}/sources/{source}/draft-config",
    response_model=ApiResponse[UploadFileSchema],
)
def upsert_upload_draft_config(
    session_id: str,
    source: str,
    body: UploadDraftConfigRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    normalized_source = source.upper().strip()
    if normalized_source not in MACHINES_SOURCES_BY_NAME:
        raise HTTPException(status_code=400, detail=f"unknown source={source}")

    session = uploads_store.get_session_by_id(db, session_id)
    if not session or session.tenant_id != current_user.tenant_id:
        raise HTTPException(status_code=404, detail="upload session not found")

    try:
        file_row = uploads_store.upsert_draft_config(
            db,
            session_id=session.id,
            source=normalized_source,
            draft=body.payload,
            profile_id=body.profile_id,
            profile_version=body.profile_version,
        )
        db.commit()
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return ApiResponse(data=_to_file_schema(file_row))


@router.delete(
    "/sessions/{session_id}/sources/{source}/draft-config",
    response_model=ApiResponse[UploadFileSchema],
)
def clear_upload_draft_config(
    session_id: str,
    source: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    normalized_source = source.upper().strip()
    if normalized_source not in MACHINES_SOURCES_BY_NAME:
        raise HTTPException(status_code=400, detail=f"unknown source={source}")

    session = uploads_store.get_session_by_id(db, session_id)
    if not session or session.tenant_id != current_user.tenant_id:
        raise HTTPException(status_code=404, detail="upload session not found")

    try:
        file_row = uploads_store.clear_draft_config(
            db,
            session_id=session.id,
            source=normalized_source,
        )
        db.commit()
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return ApiResponse(data=_to_file_schema(file_row))
