"""
models_profiles.py — SQLAlchemy ORM models for CSV Tab Config Profiles.

Tables:
  - users & groups                : placeholders for permissions
  - group_memberships             : associative table
  - csv_tab_profiles              : central profile container
  - csv_tab_profile_versions      : append-only config payload versions
  - csv_tab_profile_shares        : sharing with other users/groups
"""

from __future__ import annotations

from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import relationship

from compliance_gate.infra.db.models import TimestampMixin, _uuid
from compliance_gate.infra.db.session import Base


# ─────────────────────────────────────────────────────────────────────────────
# Auth Placeholders
# ─────────────────────────────────────────────────────────────────────────────

class User(TimestampMixin, Base):
    __tablename__ = "users"

    id = Column(String(36), primary_key=True, default=_uuid)
    tenant_id = Column(
        String(36),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    email = Column(String(255), nullable=False)
    name = Column(String(255), nullable=False)

    tenant = relationship("Tenant")


class Group(TimestampMixin, Base):
    __tablename__ = "groups"

    id = Column(String(36), primary_key=True, default=_uuid)
    tenant_id = Column(
        String(36),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    name = Column(String(255), nullable=False)

    tenant = relationship("Tenant")


class GroupMembership(TimestampMixin, Base):
    __tablename__ = "group_memberships"

    id = Column(String(36), primary_key=True, default=_uuid)
    user_id = Column(
        String(36),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    group_id = Column(
        String(36),
        ForeignKey("groups.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# CSV Tab Profiles
# ─────────────────────────────────────────────────────────────────────────────

class CsvTabProfile(TimestampMixin, Base):
    """
    Main container for a parsing configuration profile.
    Scope: PRIVATE | TEAM | TENANT | GLOBAL
    Source: AD | UEM | EDR | ASSET
    """
    __tablename__ = "csv_tab_profiles"

    id = Column(String(36), primary_key=True, default=_uuid)
    tenant_id = Column(
        String(36),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    source = Column(String(16), nullable=False, index=True)         # Target CSV source
    scope = Column(String(16), nullable=False, default="PRIVATE")   # Visibility
    owner_user_id = Column(
        String(36),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    name = Column(String(255), nullable=False)                      # Human readable name
    active_version = Column(Integer, nullable=False, default=1)     # Pointer to current version
    is_default_for_source = Column(Boolean, nullable=False, default=False)

    tenant = relationship("Tenant")
    owner = relationship("User")
    versions = relationship(
        "CsvTabProfileVersion",
        back_populates="profile",
        cascade="all, delete-orphan",
        order_by="desc(CsvTabProfileVersion.version)",
    )


class CsvTabProfileVersion(TimestampMixin, Base):
    """
    Append-only versions of the config payload.
    payload_json contains CsvTabConfig dump (header_row, selected_columns...).
    """
    __tablename__ = "csv_tab_profile_versions"

    id = Column(String(36), primary_key=True, default=_uuid)
    profile_id = Column(
        String(36),
        ForeignKey("csv_tab_profiles.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    version = Column(Integer, nullable=False)
    payload_json = Column(Text, nullable=False)                     # The mapping profile specs
    created_by = Column(
        String(36),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    change_note = Column(Text, nullable=True)

    profile = relationship("CsvTabProfile", back_populates="versions")


class CsvTabProfileShare(TimestampMixin, Base):
    """
    ACL list for TEAM or custom shared PRIVATE profiles.
    permission: READ | WRITE
    """
    __tablename__ = "csv_tab_profile_shares"

    id = Column(String(36), primary_key=True, default=_uuid)
    profile_id = Column(
        String(36),
        ForeignKey("csv_tab_profiles.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    user_id = Column(
        String(36),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    group_id = Column(
        String(36),
        ForeignKey("groups.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    permission = Column(String(16), nullable=False, default="READ")

    profile = relationship("CsvTabProfile")
