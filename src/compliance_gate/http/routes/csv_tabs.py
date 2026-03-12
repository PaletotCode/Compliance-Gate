"""
csv_tabs.py — Endpoints for managing CSV Tab Configs (Profiles).

Enables frontend to preview raw headers, parsed samples, create profiles,
and share them across tenants/users.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session

from compliance_gate.authentication.http.dependencies import require_role
from compliance_gate.authentication.models import Role, User
from compliance_gate.authentication.storage import repo as auth_repo
from compliance_gate.infra.db.session import get_db
from compliance_gate.domains.machines.ingest.mapping_profile import (
    CsvTabConfig,
    CsvTabDraftConfig,
    CsvTabProfileSchema,
)
from compliance_gate.domains.machines.ingest.sources import MACHINES_SOURCES_BY_NAME
from compliance_gate.infra.storage.data_dir_resolver import resolve_data_dir
from compliance_gate.infra.storage import profiles_store, uploads_store
from compliance_gate.domains.machines.ingest import preview

router = APIRouter(prefix="/csv-tabs", tags=["CSV Tabs"])


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints: Sources & Profiles
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/sources", response_model=list[str])
def list_sources(_: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR))) -> list[str]:
    """Returns the list of available source kinds."""
    return ["AD", "UEM", "EDR", "ASSET"]


@router.get("/profiles", response_model=list[CsvTabProfileSchema])
def list_profiles(
    source: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    """List profiles visible to the user in the tenant."""
    profiles = profiles_store.list_profiles(
        db,
        current_user.tenant_id,
        source=source,
        user_id=current_user.id,
    )
    
    # We do not include the full payload in the list for lighter payload
    return [CsvTabProfileSchema.model_validate(p) for p in profiles]


@router.get("/profiles/{profile_id}", response_model=CsvTabProfileSchema)
def get_profile(
    profile_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    profile = profiles_store.get_profile_by_id(db, profile_id)
    if not profile or profile.tenant_id != current_user.tenant_id:
        raise HTTPException(status_code=404, detail="Profile not found")

    payload = profiles_store.get_active_payload(db, profile_id)
    result = CsvTabProfileSchema.model_validate(profile)
    result.payload = payload
    return result


class CreateProfileRequest(BaseModel):
    source: str
    scope: str = "PRIVATE"
    name: str
    payload: CsvTabConfig
    is_default_for_source: bool = False

@router.post("/profiles", response_model=CsvTabProfileSchema, status_code=status.HTTP_201_CREATED)
def create_profile(
    req: CreateProfileRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    profile = profiles_store.create_profile(
        db,
        tenant_id=current_user.tenant_id,
        source=req.source,
        scope=req.scope,
        name=req.name,
        owner_user_id=current_user.id,
        payload=req.payload,
    )
    if req.is_default_for_source:
        profile = profiles_store.promote_to_default(db, profile.id)

    db.commit()

    result = CsvTabProfileSchema.model_validate(profile)
    result.payload = req.payload
    return result


class UpdateProfileRequest(BaseModel):
    payload: CsvTabConfig
    change_note: Optional[str] = None


class RenameProfileRequest(BaseModel):
    name: str


@router.put("/profiles/{profile_id}")
def update_profile(
    profile_id: str,
    req: UpdateProfileRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    try:
        profile = profiles_store.get_profile_by_id(db, profile_id)
        if not profile or profile.tenant_id != current_user.tenant_id:
            raise HTTPException(status_code=404, detail="Profile not found")
        profiles_store.update_profile_payload(
            db,
            profile_id,
            new_payload=req.payload,
            change_note=req.change_note,
            actor_user_id=current_user.id,
        )
        db.commit()
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    updated = profiles_store.get_profile_by_id(db, profile_id)
    payload = profiles_store.get_active_payload(db, profile_id)
    result = CsvTabProfileSchema.model_validate(updated)
    result.payload = payload
    return {
        "status": "ok",
        "message": "Appended new version",
        "profile": result.model_dump(mode="json"),
    }


@router.patch("/profiles/{profile_id}/rename", response_model=CsvTabProfileSchema)
def rename_profile(
    profile_id: str,
    req: RenameProfileRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    if not req.name or not req.name.strip():
        raise HTTPException(status_code=400, detail="name cannot be empty")

    profile = profiles_store.get_profile_by_id(db, profile_id)
    if not profile or profile.tenant_id != current_user.tenant_id:
        raise HTTPException(status_code=404, detail="Profile not found")

    try:
        renamed = profiles_store.rename_profile(db, profile_id, new_name=req.name)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    auth_repo.append_auth_audit(
        db,
        tenant_id=current_user.tenant_id,
        user_id=current_user.id,
        action="PROFILE_RENAME",
        meta={
            "profile_hash": auth_repo.hash_identifier(profile_id),
            "new_name": req.name[:64],
        },
    )
    db.commit()

    result = CsvTabProfileSchema.model_validate(renamed)
    result.payload = profiles_store.get_active_payload(db, profile_id)
    return result


@router.post("/profiles/{profile_id}/promote-default")
def promote_default(
    profile_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    try:
        profile = profiles_store.get_profile_by_id(db, profile_id)
        if not profile or profile.tenant_id != current_user.tenant_id:
            raise HTTPException(status_code=404, detail="Profile not found")
        profiles_store.promote_to_default(db, profile_id)
        db.commit()
        return {"status": "ok", "message": "Promoted to default"}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/profiles/{profile_id}/share")
def share_profile(
    profile_id: str,
    target_user_id: str,
    permission: str = "READ",
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    profile = profiles_store.get_profile_by_id(db, profile_id)
    if not profile or profile.tenant_id != current_user.tenant_id:
        raise HTTPException(status_code=404, detail="Profile not found")
    # Stub for future auth ACL updates
    return {"status": "ok", "message": "Stub: share ok"}


@router.delete("/profiles/{profile_id}")
def delete_profile(
    profile_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN)),
):
    profile = profiles_store.get_profile_by_id(db, profile_id)
    if not profile or profile.tenant_id != current_user.tenant_id:
        raise HTTPException(status_code=404, detail="Profile not found")

    try:
        profiles_store.delete_profile(db, profile_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    auth_repo.append_auth_audit(
        db,
        tenant_id=current_user.tenant_id,
        user_id=current_user.id,
        action="PROFILE_DELETE",
        meta={"profile_hash": auth_repo.hash_identifier(profile_id)},
    )
    db.commit()
    return {"status": "ok", "message": "Profile deleted"}


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints: Previews
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_file(source: str, parent: Path) -> Path:
    """Finds the file inside the dir for raw/parsed endpoint (single source)."""
    # For MVP we reuse pipeline's sources logic to resolve the exact file name
    from compliance_gate.domains.machines.ingest.sources import MACHINES_SOURCES_BY_NAME
    src_def = MACHINES_SOURCES_BY_NAME.get(source)
    if not src_def:
        raise HTTPException(status_code=400, detail=f"Unknown source: {source}")
    
    found_path = None
    for candidate in src_def.filename_candidates:
        p = parent / candidate
        if p.exists():
            found_path = p
            break
        # Case insensitive
        for existing in parent.iterdir():
            if existing.name.upper() == candidate.upper():
                found_path = existing
                break
        if found_path:
            break
            
    if not found_path:
        raise HTTPException(status_code=404, detail=f"No {source} file found in {parent}")
    return found_path


class PreviewRawRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    source: str
    data_dir: Optional[str] = None
    upload_session_id: Optional[str] = None
    header_row_override: Optional[int] = None
    payload: CsvTabDraftConfig | None = None
    profile_id: str | None = None
    persist_draft: bool = True
    # Compatibility aliases expected by different frontend payloads.
    config: CsvTabDraftConfig | None = None
    draft_config: CsvTabDraftConfig | None = None


def _normalize_source(source: str) -> str:
    normalized = source.upper().strip()
    if normalized not in MACHINES_SOURCES_BY_NAME:
        raise HTTPException(status_code=400, detail=f"Unknown source: {source}")
    return normalized


def _resolve_session_or_404(db: Session, *, session_id: str, tenant_id: str):
    session = uploads_store.get_session_by_id(db, session_id)
    if not session or session.tenant_id != tenant_id:
        raise HTTPException(status_code=404, detail="upload session not found")
    return session


def _extract_draft_from_raw_request(req: PreviewRawRequest) -> CsvTabDraftConfig | None:
    if req.payload is not None:
        return req.payload
    if req.config is not None:
        return req.config
    if req.draft_config is not None:
        return req.draft_config

    extra = req.model_extra or {}
    for key in ("payload", "config", "draft_config"):
        candidate = extra.get(key)
        if isinstance(candidate, dict):
            try:
                return CsvTabDraftConfig.model_validate(candidate)
            except Exception:
                continue

    inferred: dict = {}
    for key in (
        "header_row",
        "header_row_index",
        "delimiter",
        "encoding",
        "sic_column",
        "selected_columns",
        "alias_map",
        "normalize_key_strategy",
    ):
        if key in extra:
            inferred[key] = extra[key]
    if req.header_row_override is not None:
        inferred["header_row"] = req.header_row_override
    if inferred:
        return CsvTabDraftConfig.model_validate(inferred)
    return None

@router.post("/preview/raw")
def preview_raw(
    req: PreviewRawRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    source = _normalize_source(req.source)
    d = resolve_data_dir(
        db=db,
        tenant_id=current_user.tenant_id,
        data_dir=req.data_dir,
        upload_session_id=req.upload_session_id,
    )
    f = _resolve_file(source, d)

    draft = _extract_draft_from_raw_request(req)
    if req.upload_session_id and draft and req.persist_draft and Role(current_user.role) == Role.TI_ADMIN:
        session = _resolve_session_or_404(
            db,
            session_id=req.upload_session_id,
            tenant_id=current_user.tenant_id,
        )
        try:
            uploads_store.upsert_draft_config(
                db,
                session_id=session.id,
                source=source,
                draft=draft,
                profile_id=req.profile_id,
            )
            db.commit()
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    header_row = req.header_row_override
    if header_row is None and draft and draft.header_row is not None:
        header_row = draft.header_row

    result = preview.preview_raw(
        source,
        f,
        header_row_override=header_row,
        delimiter_override=draft.delimiter if draft else None,
        encoding_override=draft.encoding if draft else None,
    )
    if draft:
        result["draft_config"] = draft.model_dump(exclude_none=True)
    return result


class PreviewParsedRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    source: str
    profile_id: str | None = None
    payload: CsvTabDraftConfig | None = None
    config: CsvTabDraftConfig | None = None
    draft_config: CsvTabDraftConfig | None = None
    persist_draft: bool = True
    data_dir: Optional[str] = None
    upload_session_id: Optional[str] = None

@router.post("/preview/parsed")
def preview_parsed(
    req: PreviewParsedRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(Role.TI_ADMIN, Role.DIRECTOR)),
):
    d = resolve_data_dir(
        db=db,
        tenant_id=current_user.tenant_id,
        data_dir=req.data_dir,
        upload_session_id=req.upload_session_id,
    )
    source = _normalize_source(req.source)
    f = _resolve_file(source, d)

    complete_config: CsvTabConfig | None = None
    draft: CsvTabDraftConfig | None = req.payload or req.config or req.draft_config

    if draft is None:
        extra = req.model_extra or {}
        for key in ("payload", "config", "draft_config"):
            candidate = extra.get(key)
            if isinstance(candidate, dict):
                try:
                    draft = CsvTabDraftConfig.model_validate(candidate)
                    break
                except Exception:
                    continue

    if req.profile_id:
        profile = profiles_store.get_profile_by_id(db, req.profile_id)
        if not profile or profile.tenant_id != current_user.tenant_id:
            raise HTTPException(status_code=404, detail=f"Profile {req.profile_id} not found")
        complete_config = profiles_store.get_active_payload(db, req.profile_id)
        if not complete_config:
            raise HTTPException(status_code=404, detail=f"Active payload for {req.profile_id} not found")

    if complete_config is None and req.upload_session_id:
        session = _resolve_session_or_404(
            db,
            session_id=req.upload_session_id,
            tenant_id=current_user.tenant_id,
        )
        drafts = uploads_store.get_draft_configs(db, session_id=session.id)
        complete_config = drafts.get(source).to_complete_config() if source in drafts else None

    if draft is not None:
        if req.upload_session_id and req.persist_draft and Role(current_user.role) == Role.TI_ADMIN:
            session = _resolve_session_or_404(
                db,
                session_id=req.upload_session_id,
                tenant_id=current_user.tenant_id,
            )
            try:
                uploads_store.upsert_draft_config(
                    db,
                    session_id=session.id,
                    source=source,
                    draft=draft,
                    profile_id=req.profile_id,
                )
                db.commit()
            except ValueError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc

        complete_config = draft.to_complete_config(complete_config)

    if complete_config is None:
        raise HTTPException(
            status_code=422,
            detail=(
                "Configuração incompleta para preview parsed. "
                "Defina ao menos 'sic_column' no draft ou informe 'profile_id'."
            ),
        )

    result = preview.preview_parsed(source, f, complete_config)
    result["config_applied"] = complete_config.model_dump()
    return result
