from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from compliance_gate.authentication.models import AuthAudit, RecoveryCode, Role, Tenant, User
from compliance_gate.shared.utils.hashing import generate_hash

log = logging.getLogger(__name__)


def _slugify(text_value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", text_value.strip().lower()).strip("-")
    return cleaned or "tenant"


def _truncate_meta(meta: dict[str, Any] | None) -> str:
    payload = json.dumps(meta or {}, ensure_ascii=False, default=str)
    if len(payload) <= 1024:
        return payload
    return payload[:1024]


def acquire_user_advisory_lock(db: Session, user_id: str) -> None:
    """Best-effort advisory lock for concurrent password/MFA mutations."""
    if db.bind is None or db.bind.dialect.name != "postgresql":
        return
    key_int = int(generate_hash(f"auth-user-lock:{user_id}")[:15], 16)
    db.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": key_int})


def get_or_create_tenant(
    db: Session,
    *,
    tenant_id: str | None = None,
    tenant_name: str | None = None,
    tenant_slug: str | None = None,
) -> Tenant:
    if tenant_id:
        tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
        if tenant:
            return tenant

    if tenant_slug:
        tenant = db.query(Tenant).filter(Tenant.slug == tenant_slug).first()
        if tenant:
            return tenant

    resolved_name = tenant_name or "Default Tenant"
    slug = tenant_slug or _slugify(resolved_name)

    tenant = db.query(Tenant).filter(Tenant.slug == slug).first()
    if tenant:
        return tenant

    tenant = Tenant(
        id=tenant_id,
        slug=slug,
        display_name=resolved_name,
        name=resolved_name,
        is_active=True,
    )
    db.add(tenant)
    db.flush()
    return tenant


def get_user_by_id(db: Session, user_id: str) -> User | None:
    return db.query(User).filter(User.id == user_id).first()


def get_user_by_tenant_username(db: Session, tenant_id: str, username: str) -> User | None:
    return (
        db.query(User)
        .filter(User.tenant_id == tenant_id, User.username == username.lower())
        .first()
    )


def get_users_by_username(db: Session, username: str) -> list[User]:
    return db.query(User).filter(User.username == username.lower()).all()


def create_user(
    db: Session,
    *,
    tenant_id: str,
    username: str,
    password_hash: str,
    role: Role,
    require_password_change: bool = False,
) -> User:
    user = User(
        tenant_id=tenant_id,
        username=username.lower(),
        password_hash=password_hash,
        role=role.value,
        is_active=True,
        mfa_enabled=False,
        require_password_change=require_password_change,
    )
    db.add(user)
    db.flush()
    return user


def update_password(
    db: Session,
    *,
    user: User,
    password_hash: str,
    require_password_change: bool,
) -> None:
    acquire_user_advisory_lock(db, user.id)
    user.password_hash = password_hash
    user.require_password_change = require_password_change
    user.updated_at = datetime.now(UTC)
    db.flush()


def set_user_mfa_secret(db: Session, *, user: User, protected_secret: str) -> None:
    acquire_user_advisory_lock(db, user.id)
    user.mfa_secret_protected = protected_secret
    user.mfa_enabled = True
    user.updated_at = datetime.now(UTC)
    db.flush()


def replace_recovery_codes(db: Session, *, user_id: str, code_hashes: list[str]) -> None:
    acquire_user_advisory_lock(db, user_id)
    db.query(RecoveryCode).filter(RecoveryCode.user_id == user_id).delete()
    for code_hash in code_hashes:
        db.add(RecoveryCode(user_id=user_id, code_hash=code_hash))
    db.flush()


def consume_recovery_code(db: Session, *, user_id: str, code_hash: str) -> bool:
    acquire_user_advisory_lock(db, user_id)
    row = (
        db.query(RecoveryCode)
        .filter(
            RecoveryCode.user_id == user_id,
            RecoveryCode.code_hash == code_hash,
            RecoveryCode.used_at.is_(None),
        )
        .order_by(RecoveryCode.created_at.asc())
        .first()
    )
    if not row:
        return False
    row.used_at = datetime.now(UTC)
    db.flush()
    return True


def append_auth_audit(
    db: Session,
    *,
    tenant_id: str | None,
    user_id: str | None,
    action: str,
    meta: dict[str, Any] | None = None,
) -> None:
    entry = AuthAudit(
        tenant_id=tenant_id,
        user_id=user_id,
        action=action,
        meta_json=_truncate_meta(meta),
    )
    db.add(entry)
    # Do not flush aggressively for audit-only events.


def hash_identifier(value: str) -> str:
    return generate_hash(value)[:16]
