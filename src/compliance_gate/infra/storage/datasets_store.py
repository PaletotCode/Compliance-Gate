"""
datasets_store.py — Persistence layer for dataset_versions, dataset_files, and metrics.

All DB writes go through this module. The ingest pipeline returns IngestMetrics;
this module translates them into ORM objects and commits.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from sqlalchemy.orm import Session

from compliance_gate.infra.db.models import (
    AuditLog,
    DatasetFile,
    DatasetMetric,
    DatasetVersion,
    Tenant,
)

log = logging.getLogger(__name__)

_DEFAULT_TENANT_SLUG = "default"
_DEFAULT_TENANT_NAME = "Default Tenant"


# ─────────────────────────────────────────────────────────────────────────────
# Tenant helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_or_create_tenant(db: Session, tenant_id: Optional[str] = None) -> Tenant:
    """
    Return the requested tenant, or fall back to the default tenant.
    Creates the default tenant if it doesn't exist yet.
    """
    if tenant_id:
        t = db.query(Tenant).filter(Tenant.id == tenant_id).first()
        if t:
            return t
        log.warning("Tenant id=%s not found — falling back to default", tenant_id)

    t = db.query(Tenant).filter(Tenant.slug == _DEFAULT_TENANT_SLUG).first()
    if t:
        return t

    # Bootstrap default tenant
    t = Tenant(slug=_DEFAULT_TENANT_SLUG, display_name=_DEFAULT_TENANT_NAME)
    db.add(t)
    db.flush()
    log.info("Bootstrapped default tenant id=%s", t.id)
    return t


# ─────────────────────────────────────────────────────────────────────────────
# DatasetVersion lifecycle
# ─────────────────────────────────────────────────────────────────────────────

def create_dataset_version(
    db: Session,
    *,
    tenant_id: Optional[str] = None,
    source_type: str = "machines",
    data_dir: Optional[str] = None,
    profile_ids_map: Optional[dict[str, str]] = None,
    actor: str = "system",
) -> DatasetVersion:
    """Create a new DatasetVersion in 'pending' status."""
    tenant = get_or_create_tenant(db, tenant_id)

    version = DatasetVersion(
        tenant_id=tenant.id,
        source_type=source_type,
        status="pending",
        data_dir=data_dir,
        used_profile_ids=json.dumps(profile_ids_map or {}),
    )
    db.add(version)
    db.flush()

    _audit(db, tenant.id, version.id, actor, "create", "dataset_version", version.id)

    log.info("Created dataset_version id=%s tenant=%s", version.id, tenant.id)
    return version


def finalize_dataset_version(
    db: Session,
    version: DatasetVersion,
    *,
    status: str,
    actor: str = "system",
) -> DatasetVersion:
    """Mark a version as 'success' or 'failed'."""
    version.status = status
    db.flush()
    _audit(db, version.tenant_id, version.id, actor, "update", "dataset_version", version.id,
           {"status": status})
    return version


# ─────────────────────────────────────────────────────────────────────────────
# DatasetFile registration
# ─────────────────────────────────────────────────────────────────────────────

def register_file(
    db: Session,
    *,
    version_id: str,
    source: str,
    original_filename: Optional[str] = None,
    resolved_path: Optional[str] = None,
    checksum_sha256: Optional[str] = None,
    file_size_bytes: Optional[int] = None,
    detected_encoding: Optional[str] = None,
    detected_delimiter: Optional[str] = None,
    header_row_index: Optional[int] = None,
    detected_headers: Optional[list[str]] = None,
    rows_read: Optional[int] = None,
    rows_valid: Optional[int] = None,
    parse_warnings: Optional[list[str]] = None,
) -> DatasetFile:
    """Register one source file into dataset_files for a given version."""
    f = DatasetFile(
        version_id=version_id,
        source=source,
        original_filename=original_filename,
        resolved_path=resolved_path,
        checksum_sha256=checksum_sha256,
        file_size_bytes=file_size_bytes,
        detected_encoding=detected_encoding,
        detected_delimiter=detected_delimiter,
        header_row_index=header_row_index,
        detected_headers=json.dumps(detected_headers or [], ensure_ascii=False),
        rows_read=rows_read,
        rows_valid=rows_valid,
        parse_warnings=json.dumps(parse_warnings or [], ensure_ascii=False),
    )
    db.add(f)
    db.flush()
    log.debug("Registered file source=%s version=%s rows=%s", source, version_id, rows_read)
    return f


# ─────────────────────────────────────────────────────────────────────────────
# Metrics persistence
# ─────────────────────────────────────────────────────────────────────────────

def save_metrics(
    db: Session,
    *,
    version_id: str,
    total_entries: int,
    from_ad: int,
    from_uem: int,
    from_edr: int,
    match_ad_uem: int,
    match_ad_edr: int,
    asset_matched: int,
    cloned_serials: int,
    rows_read_total: int,
    rows_valid_total: int,
    total_elapsed_ms: float,
    warnings_count: int,
) -> DatasetMetric:
    """Create or replace metrics for a dataset_version."""
    # Remove existing if any
    db.query(DatasetMetric).filter(DatasetMetric.version_id == version_id).delete()

    parse_rate = rows_valid_total / rows_read_total if rows_read_total > 0 else 0.0
    cross_source = match_ad_uem + match_ad_edr
    match_rate = cross_source / total_entries if total_entries > 0 else 0.0

    m = DatasetMetric(
        version_id=version_id,
        total_entries=total_entries,
        from_ad=from_ad,
        from_uem=from_uem,
        from_edr=from_edr,
        match_ad_uem=match_ad_uem,
        match_ad_edr=match_ad_edr,
        asset_matched=asset_matched,
        cloned_serials=cloned_serials,
        rows_read_total=rows_read_total,
        rows_valid_total=rows_valid_total,
        parse_rate=parse_rate,
        match_rate=match_rate,
        total_elapsed_ms=total_elapsed_ms,
        warnings_count=warnings_count,
    )
    db.add(m)
    db.flush()
    log.info(
        "Metrics saved version=%s entries=%d parse_rate=%.2f%% match_rate=%.2f%%",
        version_id, total_entries,
        parse_rate * 100, match_rate * 100,
    )
    return m


# ─────────────────────────────────────────────────────────────────────────────
# Queries
# ─────────────────────────────────────────────────────────────────────────────

def get_latest_version(
    db: Session,
    tenant_id: Optional[str] = None,
    source_type: str = "machines",
) -> Optional[DatasetVersion]:
    """Return the most recent successful dataset_version for a tenant."""
    tenant = get_or_create_tenant(db, tenant_id)
    return (
        db.query(DatasetVersion)
        .filter(
            DatasetVersion.tenant_id == tenant.id,
            DatasetVersion.source_type == source_type,
            DatasetVersion.status == "success",
        )
        .order_by(DatasetVersion.created_at.desc())
        .first()
    )


def get_version_by_id(
    db: Session,
    version_id: str,
) -> Optional[DatasetVersion]:
    return db.query(DatasetVersion).filter(DatasetVersion.id == version_id).first()


def list_versions(
    db: Session,
    tenant_id: Optional[str] = None,
    source_type: str = "machines",
    limit: int = 20,
    offset: int = 0,
) -> tuple[list[DatasetVersion], int]:
    tenant = get_or_create_tenant(db, tenant_id)
    q = (
        db.query(DatasetVersion)
        .filter(
            DatasetVersion.tenant_id == tenant.id,
            DatasetVersion.source_type == source_type,
        )
        .order_by(DatasetVersion.created_at.desc())
    )
    total = q.count()
    return q.offset(offset).limit(limit).all(), total


# ─────────────────────────────────────────────────────────────────────────────
# Audit helper (internal)
# ─────────────────────────────────────────────────────────────────────────────

def _audit(
    db: Session,
    tenant_id: Optional[str],
    dataset_version_id: Optional[str],
    actor: str,
    action: str,
    entity_type: str,
    entity_id: str,
    details: Optional[dict] = None,
) -> AuditLog:
    entry = AuditLog(
        tenant_id=tenant_id,
        dataset_version_id=dataset_version_id,
        actor=actor,
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        details=json.dumps(details or {}, ensure_ascii=False, default=str),
    )
    db.add(entry)
    return entry
