from __future__ import annotations

import uuid
from enum import StrEnum

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import relationship

from compliance_gate.infra.db.session import Base


def _uuid() -> str:
    return str(uuid.uuid4())


class TimestampMixin:
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=True,
    )


class Role(StrEnum):
    TI_ADMIN = "TI_ADMIN"
    DIRECTOR = "DIRECTOR"
    TI_OPERATOR = "TI_OPERATOR"
    AUDITOR = "AUDITOR"


class Tenant(TimestampMixin, Base):
    __tablename__ = "tenants"

    id = Column(String(36), primary_key=True, default=_uuid)
    # Legacy columns kept for backward compatibility with existing dataset/profile flows.
    slug = Column(String(64), unique=True, nullable=False)
    display_name = Column(String(256), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)

    # Canonical name expected by AUTH CORE v1.
    name = Column(String(256), nullable=False)

    users = relationship("User", back_populates="tenant")
    dataset_versions = relationship("DatasetVersion", back_populates="tenant")
    audit_logs = relationship("AuditLog", back_populates="tenant")
    auth_audits = relationship("AuthAudit", back_populates="tenant")


class User(TimestampMixin, Base):
    __tablename__ = "users"
    __table_args__ = (
        UniqueConstraint("tenant_id", "username", name="uq_users_tenant_username"),
        Index("ix_users_tenant_username", "tenant_id", "username"),
    )

    id = Column(String(36), primary_key=True, default=_uuid)
    tenant_id = Column(
        String(36),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    username = Column(String(128), nullable=False)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(32), nullable=False, default=Role.DIRECTOR.value)
    is_active = Column(Boolean, nullable=False, default=True)

    mfa_enabled = Column(Boolean, nullable=False, default=False)
    mfa_secret_protected = Column(Text, nullable=True)
    require_password_change = Column(Boolean, nullable=False, default=False)

    # Legacy fields from previous profile placeholder table.
    email = Column(String(255), nullable=True)
    name = Column(String(255), nullable=True)

    tenant = relationship("Tenant", back_populates="users")
    recovery_codes = relationship(
        "RecoveryCode",
        back_populates="user",
        cascade="all, delete-orphan",
    )


class RecoveryCode(TimestampMixin, Base):
    __tablename__ = "recovery_codes"

    id = Column(String(36), primary_key=True, default=_uuid)
    user_id = Column(
        String(36),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    code_hash = Column(String(128), nullable=False)
    used_at = Column(DateTime(timezone=True), nullable=True)

    user = relationship("User", back_populates="recovery_codes")


class AuthAudit(Base):
    __tablename__ = "auth_audit"

    id = Column(String(36), primary_key=True, default=_uuid)
    tenant_id = Column(
        String(36),
        ForeignKey("tenants.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    user_id = Column(
        String(36),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    action = Column(String(64), nullable=False)
    meta_json = Column(Text, nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    tenant = relationship("Tenant", back_populates="auth_audits")
    user = relationship("User")
