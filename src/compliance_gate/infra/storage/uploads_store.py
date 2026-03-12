"""
uploads_store.py — Persistence layer for controlled workspace upload sessions.

Stores tenant-scoped upload sessions and per-source file metadata.
"""

from __future__ import annotations

import json
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session, joinedload

from compliance_gate.domains.machines.ingest.mapping_profile import CsvTabDraftConfig
from compliance_gate.infra.db.models import (
    AuditLog,
    WorkspaceUploadFile,
    WorkspaceUploadSession,
)
from compliance_gate.shared.utils.hashing import generate_hash


def acquire_session_advisory_lock(db: Session, session_id: str) -> None:
    """Best-effort session lock for destructive operations."""
    if db.bind is None or db.bind.dialect.name != "postgresql":
        return
    key_int = int(generate_hash(f"workspace-upload-lock:{session_id}")[:15], 16)
    db.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": key_int})


def create_upload_session(
    db: Session,
    *,
    tenant_id: str,
    created_by: str | None,
    root_path: str,
) -> WorkspaceUploadSession:
    session = WorkspaceUploadSession(
        tenant_id=tenant_id,
        created_by=created_by,
        status="active",
        root_path=root_path,
        total_files=0,
        total_bytes=0,
    )
    db.add(session)
    db.flush()
    return session


def register_upload_file(
    db: Session,
    *,
    session_id: str,
    source: str,
    original_filename: str,
    stored_filename: str,
    checksum_sha256: str,
    file_size_bytes: int,
    detected_encoding: str,
    validation_warnings: Optional[list[str]] = None,
) -> WorkspaceUploadFile:
    row = WorkspaceUploadFile(
        session_id=session_id,
        source=source,
        original_filename=original_filename,
        stored_filename=stored_filename,
        checksum_sha256=checksum_sha256,
        file_size_bytes=file_size_bytes,
        detected_encoding=detected_encoding,
        validation_warnings=json.dumps(validation_warnings or [], ensure_ascii=False),
    )
    db.add(row)
    db.flush()
    return row


def _get_session_file(db: Session, *, session_id: str, source: str) -> WorkspaceUploadFile | None:
    return (
        db.query(WorkspaceUploadFile)
        .filter(
            WorkspaceUploadFile.session_id == session_id,
            WorkspaceUploadFile.source == source,
        )
        .first()
    )


def upsert_draft_config(
    db: Session,
    *,
    session_id: str,
    source: str,
    draft: CsvTabDraftConfig,
    profile_id: str | None = None,
    profile_version: int | None = None,
) -> WorkspaceUploadFile:
    row = _get_session_file(db, session_id=session_id, source=source)
    if row is None:
        raise ValueError(f"upload file not found for source={source}")

    row.draft_config_json = json.dumps(
        draft.model_dump(exclude_none=True),
        ensure_ascii=False,
    )
    row.draft_profile_id = profile_id
    row.draft_profile_version = profile_version
    db.flush()
    return row


def clear_draft_config(db: Session, *, session_id: str, source: str) -> WorkspaceUploadFile:
    row = _get_session_file(db, session_id=session_id, source=source)
    if row is None:
        raise ValueError(f"upload file not found for source={source}")
    row.draft_config_json = None
    row.draft_profile_id = None
    row.draft_profile_version = None
    db.flush()
    return row


def get_draft_configs(db: Session, *, session_id: str) -> dict[str, CsvTabDraftConfig]:
    rows = (
        db.query(WorkspaceUploadFile)
        .filter(WorkspaceUploadFile.session_id == session_id)
        .all()
    )
    out: dict[str, CsvTabDraftConfig] = {}
    for row in rows:
        if not row.draft_config_json:
            continue
        try:
            payload = json.loads(row.draft_config_json)
            out[row.source] = CsvTabDraftConfig.model_validate(payload)
        except Exception:
            continue
    return out


def finalize_upload_session(
    db: Session,
    session: WorkspaceUploadSession,
    *,
    total_files: int,
    total_bytes: int,
    source_manifest: dict[str, str],
) -> WorkspaceUploadSession:
    session.total_files = total_files
    session.total_bytes = total_bytes
    session.source_manifest = json.dumps(source_manifest, ensure_ascii=False)
    session.status = "ready"
    db.flush()
    return session


def list_sessions(
    db: Session,
    *,
    tenant_id: str,
    limit: int = 20,
    offset: int = 0,
) -> tuple[list[WorkspaceUploadSession], int]:
    q = (
        db.query(WorkspaceUploadSession)
        .options(joinedload(WorkspaceUploadSession.files))
        .filter(WorkspaceUploadSession.tenant_id == tenant_id)
        .order_by(WorkspaceUploadSession.created_at.desc())
    )
    total = q.count()
    return q.offset(offset).limit(limit).all(), total


def get_session_by_id(db: Session, session_id: str) -> WorkspaceUploadSession | None:
    return (
        db.query(WorkspaceUploadSession)
        .options(joinedload(WorkspaceUploadSession.files))
        .filter(WorkspaceUploadSession.id == session_id)
        .first()
    )


def delete_session(db: Session, session: WorkspaceUploadSession) -> None:
    db.delete(session)
    db.flush()


def append_audit(
    db: Session,
    *,
    tenant_id: str,
    actor: str | None,
    action: str,
    entity_id: str,
    details: Optional[dict] = None,
) -> None:
    db.add(
        AuditLog(
            tenant_id=tenant_id,
            dataset_version_id=None,
            actor=actor,
            action=action,
            entity_type="workspace_upload_session",
            entity_id=entity_id,
            details=json.dumps(details or {}, ensure_ascii=False, default=str),
        )
    )
