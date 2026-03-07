"""
csv_tabs.py — Endpoints for managing CSV Tab Configs (Profiles).

Enables frontend to preview raw headers, parsed samples, create profiles,
and share them across tenants/users.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from compliance_gate.config.settings import settings
from compliance_gate.infra.db.session import get_db
from compliance_gate.domains.machines.ingest.mapping_profile import CsvTabConfig, CsvTabProfileSchema
from compliance_gate.infra.storage import profiles_store
from compliance_gate.domains.machines.ingest import preview

log = logging.getLogger(__name__)
router = APIRouter(prefix="/csv-tabs", tags=["CSV Tabs"])

# Fake user_id for MVP
MOCK_USER_ID = "00000000-0000-0000-0000-000000000001"


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints: Sources & Profiles
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/sources", response_model=list[str])
def list_sources() -> list[str]:
    """Returns the list of available source kinds."""
    return ["AD", "UEM", "EDR", "ASSET"]


@router.get("/profiles", response_model=list[CsvTabProfileSchema])
def list_profiles(source: Optional[str] = None, db: Session = Depends(get_db)):
    """List profiles visible to the user in the tenant."""
    tenant_id = settings.default_tenant_id
    profiles = profiles_store.list_profiles(db, tenant_id, source=source, user_id=MOCK_USER_ID)
    
    # We do not include the full payload in the list for lighter payload
    return [CsvTabProfileSchema.model_validate(p) for p in profiles]


@router.get("/profiles/{profile_id}", response_model=CsvTabProfileSchema)
def get_profile(profile_id: str, db: Session = Depends(get_db)):
    profile = profiles_store.get_profile_by_id(db, profile_id)
    if not profile:
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
def create_profile(req: CreateProfileRequest, db: Session = Depends(get_db)):
    tenant_id = settings.default_tenant_id
    profile = profiles_store.create_profile(
        db,
        tenant_id=tenant_id,
        source=req.source,
        scope=req.scope,
        name=req.name,
        # Set to None to avoid violating Postgres FK constraint against missing users.
        owner_user_id=None,
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

@router.put("/profiles/{profile_id}")
def update_profile(profile_id: str, req: UpdateProfileRequest, db: Session = Depends(get_db)):
    try:
        profiles_store.update_profile_payload(
            db,
            profile_id,
            new_payload=req.payload,
            change_note=req.change_note,
            actor_user_id=None,
        )
        db.commit()
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {"status": "ok", "message": "Appended new version"}


@router.post("/profiles/{profile_id}/promote-default")
def promote_default(profile_id: str, db: Session = Depends(get_db)):
    try:
        profiles_store.promote_to_default(db, profile_id)
        db.commit()
        return {"status": "ok", "message": "Promoted to default"}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/profiles/{profile_id}/share")
def share_profile(profile_id: str, target_user_id: str, permission: str = "READ", db: Session = Depends(get_db)):
    # Stub for future auth ACL updates
    return {"status": "ok", "message": "Stub: share ok"}


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints: Previews
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_dir(path_str: Optional[str]) -> Path:
    d = Path(path_str) if path_str else Path(settings.cg_data_dir)
    if not d.exists():
        raise HTTPException(status_code=400, detail=f"Directory {d} does not exist.")
    return d

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
    source: str
    data_dir: Optional[str] = None
    header_row_override: Optional[int] = None

@router.post("/preview/raw")
def preview_raw(req: PreviewRawRequest):
    d = _resolve_dir(req.data_dir)
    f = _resolve_file(req.source, d)
    
    return preview.preview_raw(
        req.source,
        f,
        header_row_override=req.header_row_override,
    )


class PreviewParsedRequest(BaseModel):
    source: str
    profile_id: str
    data_dir: Optional[str] = None

@router.post("/preview/parsed")
def preview_parsed(req: PreviewParsedRequest, db: Session = Depends(get_db)):
    d = _resolve_dir(req.data_dir)
    f = _resolve_file(req.source, d)

    payload = profiles_store.get_active_payload(db, req.profile_id)
    if not payload:
        raise HTTPException(status_code=404, detail=f"Active payload for {req.profile_id} not found")

    return preview.preview_parsed(req.source, f, payload)
