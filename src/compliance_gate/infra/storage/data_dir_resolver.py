"""
data_dir_resolver.py — Safe resolution of ingest directories.

Rules:
  - Preferred input is upload_session_id (tenant-scoped).
  - Explicit data_dir is accepted only inside allowlisted roots.
  - No arbitrary paths from clients.
"""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import HTTPException
from sqlalchemy.orm import Session

from compliance_gate.config.settings import settings
from compliance_gate.infra.storage import uploads_store


def _is_under_any(path: Path, bases: list[Path]) -> bool:
    for base in bases:
        try:
            if path.is_relative_to(base):
                return True
        except Exception:
            continue
    return False


def _allowlisted_roots() -> list[Path]:
    roots = []
    for raw in [settings.cg_data_dir, settings.cg_upload_dir, "/workspace"]:
        if raw:
            roots.append(Path(raw).resolve())
    # Remove duplicates while preserving order
    seen: set[str] = set()
    unique_roots: list[Path] = []
    for r in roots:
        key = str(r)
        if key not in seen:
            seen.add(key)
            unique_roots.append(r)
    return unique_roots


def resolve_data_dir(
    *,
    db: Session,
    tenant_id: str,
    data_dir: str | None,
    upload_session_id: str | None,
) -> Path:
    """
    Resolve a safe path for preview/ingest.
    """
    if upload_session_id:
        session = uploads_store.get_session_by_id(db, upload_session_id)
        if not session or session.tenant_id != tenant_id:
            raise HTTPException(status_code=404, detail="upload_session not found")
        if session.status != "ready":
            raise HTTPException(status_code=409, detail="upload_session is not ready")
        p = Path(session.root_path).resolve()
        if not p.exists():
            raise HTTPException(status_code=422, detail="upload_session data directory not found")
        return p

    roots = _allowlisted_roots()
    if data_dir:
        p = Path(data_dir).resolve()
        if not _is_under_any(p, roots):
            raise HTTPException(
                status_code=422,
                detail="data_dir outside allowlisted workspace roots",
            )
        if not p.exists():
            raise HTTPException(status_code=422, detail="data_dir not found")
        return p

    env_dir = os.environ.get("CG_DATA_DIR", "")
    for candidate in [env_dir if env_dir else None, settings.cg_data_dir]:
        if not candidate:
            continue
        p = Path(candidate).resolve()
        if p.exists() and _is_under_any(p, roots):
            return p

    raise HTTPException(
        status_code=422,
        detail=(
            "data_dir não encontrado. Informe 'upload_session_id' "
            "ou use um diretório dentro da allowlist de workspace."
        ),
    )
