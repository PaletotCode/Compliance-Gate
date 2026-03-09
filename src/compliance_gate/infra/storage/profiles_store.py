"""
profiles_store.py — Persistence layer for CsvTabProfiles.

Handles CRUD operations, version append, default promotion, and sharing.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from sqlalchemy.orm import Session

from compliance_gate.infra.db.models_profiles import (
    CsvTabProfile,
    CsvTabProfileShare,
    CsvTabProfileVersion,
)
from compliance_gate.infra.storage.datasets_store import get_or_create_tenant
from compliance_gate.domains.machines.ingest.mapping_profile import CsvTabConfig

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CRUD Operations
# ─────────────────────────────────────────────────────────────────────────────

def create_profile(
    db: Session,
    *,
    tenant_id: Optional[str],
    source: str,
    scope: str,
    name: str,
    owner_user_id: Optional[str],
    payload: CsvTabConfig,
    actor: str = "system",
) -> CsvTabProfile:
    """Creates a new profile and its version 1 payload."""
    tenant = get_or_create_tenant(db, tenant_id)

    profile = CsvTabProfile(
        tenant_id=tenant.id,
        source=source,
        scope=scope,
        owner_user_id=owner_user_id,
        name=name,
        active_version=1,
        is_default_for_source=False,
    )
    db.add(profile)
    db.flush()

    version = CsvTabProfileVersion(
        profile_id=profile.id,
        version=1,
        payload_json=payload.model_dump_json(),
        created_by=owner_user_id,
    )
    db.add(version)
    db.flush()
    log.info("Created CsvTabProfile id=%s source=%s v1", profile.id, source)
    return profile


def update_profile_payload(
    db: Session,
    profile_id: str,
    *,
    new_payload: CsvTabConfig,
    change_note: Optional[str] = None,
    actor_user_id: Optional[str] = None,
) -> CsvTabProfileVersion:
    """Appends a new version to the profile and increments active_version."""
    profile = db.query(CsvTabProfile).filter(CsvTabProfile.id == profile_id).first()
    if not profile:
        raise ValueError(f"Profile {profile_id} not found")

    next_v = profile.active_version + 1
    version = CsvTabProfileVersion(
        profile_id=profile.id,
        version=next_v,
        payload_json=new_payload.model_dump_json(),
        created_by=actor_user_id,
        change_note=change_note,
    )
    db.add(version)
    
    profile.active_version = next_v
    db.flush()
    log.info("Appended v%d to CsvTabProfile id=%s", next_v, profile_id)
    return version


def get_profile_by_id(db: Session, profile_id: str) -> Optional[CsvTabProfile]:
    return db.query(CsvTabProfile).filter(CsvTabProfile.id == profile_id).first()


def get_active_payload(db: Session, profile_id: str) -> Optional[CsvTabConfig]:
    """Retrieves the active parsed JSON payload for a profile."""
    profile = get_profile_by_id(db, profile_id)
    if not profile:
        return None

    active_v = db.query(CsvTabProfileVersion).filter(
        CsvTabProfileVersion.profile_id == profile.id,
        CsvTabProfileVersion.version == profile.active_version
    ).first()

    if not active_v:
        return None

    try:
        data = json.loads(active_v.payload_json)
        return CsvTabConfig(**data)
    except Exception as e:
        log.error("Failed to parse CsvTabProfileVersion %s: %s", profile_id, e)
        return None


def list_profiles(
    db: Session,
    tenant_id: Optional[str],
    source: Optional[str] = None,
    user_id: Optional[str] = None,
) -> list[CsvTabProfile]:
    """
    List profiles visible to the user.
    For MVP: we just return all TENANT/GLOBAL and PRIVATE owned by user.
    Sharing rules (CsvTabProfileShare) can be enforced here later.
    """
    tenant = get_or_create_tenant(db, tenant_id)
    q = db.query(CsvTabProfile).filter(CsvTabProfile.tenant_id == tenant.id)
    
    if source:
        q = q.filter(CsvTabProfile.source == source)
        
    if user_id:
        q = q.filter(
            (CsvTabProfile.scope.in_(["TENANT", "GLOBAL", "TEAM"])) |
            ((CsvTabProfile.scope == "PRIVATE") & (CsvTabProfile.owner_user_id == user_id))
        )
    
    return q.order_by(CsvTabProfile.created_at.desc()).all()


def promote_to_default(db: Session, profile_id: str) -> CsvTabProfile:
    """Sets a profile as the only default for its source within the tenant."""
    profile = db.query(CsvTabProfile).filter(CsvTabProfile.id == profile_id).first()
    if not profile:
        raise ValueError(f"Profile {profile_id} not found")

    # Demote others
    db.query(CsvTabProfile).filter(
        CsvTabProfile.tenant_id == profile.tenant_id,
        CsvTabProfile.source == profile.source,
        CsvTabProfile.id != profile.id,
    ).update({"is_default_for_source": False})

    # Promote target
    profile.is_default_for_source = True
    db.flush()
    log.info("Promoted CsvTabProfile %s to default for %s", profile.id, profile.source)
    return profile


def rename_profile(
    db: Session,
    profile_id: str,
    *,
    new_name: str,
) -> CsvTabProfile:
    profile = db.query(CsvTabProfile).filter(CsvTabProfile.id == profile_id).first()
    if not profile:
        raise ValueError(f"Profile {profile_id} not found")
    profile.name = new_name.strip()
    db.flush()
    return profile


def delete_profile(db: Session, profile_id: str) -> None:
    profile = db.query(CsvTabProfile).filter(CsvTabProfile.id == profile_id).first()
    if not profile:
        raise ValueError(f"Profile {profile_id} not found")
    db.delete(profile)
    db.flush()


def get_default_profile_payload(
    db: Session,
    tenant_id: Optional[str],
    source: str
) -> Optional[CsvTabConfig]:
    """Retrieves the active payload of the default profile for a source, if any."""
    tenant = get_or_create_tenant(db, tenant_id)
    profile = db.query(CsvTabProfile).filter(
        CsvTabProfile.tenant_id == tenant.id,
        CsvTabProfile.source == source,
        CsvTabProfile.is_default_for_source == True
    ).first()

    if profile:
        return get_active_payload(db, profile.id)
    return None
