from __future__ import annotations

import logging
from dataclasses import dataclass

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from compliance_gate.authentication.config import auth_settings
from compliance_gate.authentication.models import Role, User
from compliance_gate.authentication.schemas import UserPublic
from compliance_gate.authentication.security.passwords import hash_password
from compliance_gate.authentication.storage import repo

log = logging.getLogger(__name__)


@dataclass(slots=True)
class UserServiceError(Exception):
    message: str


class UsersService:
    @staticmethod
    def to_public(user: User) -> UserPublic:
        return UserPublic(
            id=user.id,
            tenant_id=user.tenant_id,
            username=user.username,
            role=Role(user.role),
            is_active=user.is_active,
            mfa_enabled=user.mfa_enabled,
            require_password_change=user.require_password_change,
        )

    @staticmethod
    def create_user(
        db: Session,
        *,
        actor: User,
        username: str,
        role: Role,
        password: str,
        tenant_id: str | None = None,
    ) -> User:
        target_tenant_id = tenant_id or actor.tenant_id

        existing = repo.get_user_by_tenant_username(db, target_tenant_id, username)
        if existing:
            raise UserServiceError("username already exists in tenant")

        password_hash = hash_password(password)
        user = repo.create_user(
            db,
            tenant_id=target_tenant_id,
            username=username,
            password_hash=password_hash,
            role=role,
        )
        repo.append_auth_audit(
            db,
            tenant_id=target_tenant_id,
            user_id=actor.id,
            action="USER_CREATE",
            meta={"created_user_hash": repo.hash_identifier(user.id), "role": role.value},
        )
        try:
            db.commit()
        except IntegrityError as exc:
            db.rollback()
            raise UserServiceError("failed to create user due to unique constraint") from exc
        db.refresh(user)
        return user

    @staticmethod
    def admin_reset_password(
        db: Session,
        *,
        actor: User,
        target_user: User,
        new_password: str,
        require_password_change: bool,
    ) -> None:
        password_hash = hash_password(new_password)
        repo.update_password(
            db,
            user=target_user,
            password_hash=password_hash,
            require_password_change=require_password_change,
        )
        repo.append_auth_audit(
            db,
            tenant_id=target_user.tenant_id,
            user_id=actor.id,
            action="ADMIN_RESET_PASSWORD",
            meta={"target_user_hash": repo.hash_identifier(target_user.id)},
        )
        db.commit()

    @staticmethod
    def ensure_bootstrap_admin(db: Session) -> None:
        username = auth_settings.auth_bootstrap_admin_username
        password = auth_settings.auth_bootstrap_admin_password
        if not username or not password:
            return

        tenant = repo.get_or_create_tenant(
            db,
            tenant_id=auth_settings.auth_bootstrap_tenant_id,
            tenant_slug=auth_settings.auth_bootstrap_tenant_slug,
            tenant_name=auth_settings.auth_bootstrap_tenant_name,
        )

        existing = repo.get_user_by_tenant_username(db, tenant.id, username)
        if existing:
            return

        user = repo.create_user(
            db,
            tenant_id=tenant.id,
            username=username,
            password_hash=hash_password(password),
            role=Role.TI_ADMIN,
            require_password_change=False,
        )
        repo.append_auth_audit(
            db,
            tenant_id=tenant.id,
            user_id=user.id,
            action="BOOTSTRAP_ADMIN_CREATE",
            meta={"user_hash": repo.hash_identifier(user.id)},
        )
        db.commit()
        log.info("Bootstrap admin ensured for tenant=%s", repo.hash_identifier(tenant.id))
