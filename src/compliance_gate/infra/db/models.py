"""
models.py — SQLAlchemy ORM models for Compliance Gate.

Tables:
  - dataset_versions  : one row per ingest run
  - dataset_files     : one row per source file (AD/UEM/EDR/ASSET) per version
  - dataset_metrics   : aggregated metrics per dataset_version
  - audit_logs        : create/update/promote events (CCS-friendly audit trail)
"""

from __future__ import annotations

import uuid

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.orm import relationship

from .session import Base


def _uuid() -> str:
    return str(uuid.uuid4())


# ─────────────────────────────────────────────────────────────────────────────
# Base mixin
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# DatasetVersion — one row per ingest run
# ─────────────────────────────────────────────────────────────────────────────

class DatasetVersion(TimestampMixin, Base):
    __tablename__ = "dataset_versions"

    id = Column(String(36), primary_key=True, default=_uuid)
    tenant_id = Column(
        String(36),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    source_type = Column(String(32), nullable=False, default="machines")
    status = Column(String(16), nullable=False, default="pending")
    used_profile_ids = Column(Text, nullable=True)                        # dict mapping source -> CsvTabProfile.id
    data_dir = Column(Text, nullable=True)
    notes = Column(Text, nullable=True)

    tenant = relationship("Tenant", back_populates="dataset_versions")
    files = relationship(
        "DatasetFile",
        back_populates="version",
        cascade="all, delete-orphan",
    )
    metrics = relationship(
        "DatasetMetric",
        back_populates="version",
        cascade="all, delete-orphan",
        uselist=False,
    )
    audit_logs = relationship("AuditLog", back_populates="dataset_version")


# ─────────────────────────────────────────────────────────────────────────────
# DatasetFile — one row per source file per version
# ─────────────────────────────────────────────────────────────────────────────

class DatasetFile(TimestampMixin, Base):
    __tablename__ = "dataset_files"

    id = Column(String(36), primary_key=True, default=_uuid)
    version_id = Column(
        String(36),
        ForeignKey("dataset_versions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    source = Column(String(16), nullable=False)     # AD | UEM | EDR | ASSET
    original_filename = Column(String(256), nullable=True)
    resolved_path = Column(Text, nullable=True)
    checksum_sha256 = Column(String(64), nullable=True)
    file_size_bytes = Column(Integer, nullable=True)

    # Detection results
    detected_encoding = Column(String(32), nullable=True)
    detected_delimiter = Column(String(4), nullable=True)
    header_row_index = Column(Integer, nullable=True)   # 0-based row of header
    detected_headers = Column(Text, nullable=True)      # JSON list of header names

    # Parse results
    rows_read = Column(Integer, nullable=True)
    rows_valid = Column(Integer, nullable=True)
    parse_warnings = Column(Text, nullable=True)        # JSON list of warning strings

    version = relationship("DatasetVersion", back_populates="files")


# ─────────────────────────────────────────────────────────────────────────────
# DatasetMetric — aggregated ingest metrics per version
# ─────────────────────────────────────────────────────────────────────────────

class DatasetMetric(TimestampMixin, Base):
    __tablename__ = "dataset_metrics"

    id = Column(String(36), primary_key=True, default=_uuid)
    version_id = Column(
        String(36),
        ForeignKey("dataset_versions.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )

    # Universe counts (after join)
    total_entries = Column(Integer, nullable=True)
    from_ad = Column(Integer, nullable=True)
    from_uem = Column(Integer, nullable=True)
    from_edr = Column(Integer, nullable=True)

    # Join quality
    match_ad_uem = Column(Integer, nullable=True)
    match_ad_edr = Column(Integer, nullable=True)
    asset_matched = Column(Integer, nullable=True)
    cloned_serials = Column(Integer, nullable=True)

    # Parse quality (aggregate across all sources)
    rows_read_total = Column(Integer, nullable=True)
    rows_valid_total = Column(Integer, nullable=True)
    parse_rate = Column(Float, nullable=True)           # rows_valid / rows_read
    match_rate = Column(Float, nullable=True)           # entries from >1 source / total

    # Timing
    total_elapsed_ms = Column(Float, nullable=True)
    warnings_count = Column(Integer, nullable=True)

    version = relationship("DatasetVersion", back_populates="metrics")


# ─────────────────────────────────────────────────────────────────────────────
# AuditLog — CCS-friendly audit trail
# ─────────────────────────────────────────────────────────────────────────────

class AuditLog(TimestampMixin, Base):
    __tablename__ = "audit_logs"

    id = Column(String(36), primary_key=True, default=_uuid)
    tenant_id = Column(
        String(36),
        ForeignKey("tenants.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    dataset_version_id = Column(
        String(36),
        ForeignKey("dataset_versions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    actor = Column(String(128), nullable=True)      # user id / "system" / api key alias
    action = Column(String(64), nullable=False)     # "create" | "update" | "promote" | "ingest"
    entity_type = Column(String(64), nullable=True) # "dataset_version" | "dataset_file" etc.
    entity_id = Column(String(36), nullable=True)
    details = Column(Text, nullable=True)           # JSON payload (truncated, no full PII)

    tenant = relationship("Tenant", back_populates="audit_logs")
    dataset_version = relationship("DatasetVersion", back_populates="audit_logs")
