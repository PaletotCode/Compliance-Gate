from __future__ import annotations

import uuid

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import relationship

from compliance_gate.infra.db.models import TimestampMixin
from compliance_gate.infra.db.session import Base


def _uuid() -> str:
    return str(uuid.uuid4())


class EngineArtifact(TimestampMixin, Base):
    __tablename__ = "engine_artifacts"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "dataset_version_id",
            "artifact_type",
            "artifact_name",
            name="uq_engine_artifacts_key",
        ),
        Index("ix_engine_artifacts_artifact_name", "artifact_name"),
    )

    id = Column(String(36), primary_key=True, default=_uuid)
    tenant_id = Column(
        String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    dataset_version_id = Column(
        String(36),
        ForeignKey("dataset_versions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    domain = Column(String(64), nullable=False)
    artifact_type = Column(String(32), nullable=False)
    artifact_name = Column(String(128), nullable=False)
    path = Column(Text, nullable=False)
    checksum = Column(String(64), nullable=True)
    row_count = Column(Integer, nullable=True)
    schema_json = Column(Text, nullable=True)

    tenant = relationship("Tenant")
    dataset_version = relationship("DatasetVersion")


class EngineReportDefinition(TimestampMixin, Base):
    __tablename__ = "engine_report_definitions"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_engine_report_definitions_tenant_name"),
    )

    id = Column(String(36), primary_key=True, default=_uuid)
    tenant_id = Column(
        String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name = Column(String(256), nullable=False)
    scope = Column(String(32), nullable=False, default="TENANT")
    active_version = Column(Integer, default=1, nullable=False)

    tenant = relationship("Tenant")
    versions = relationship(
        "EngineReportVersion", back_populates="report", cascade="all, delete-orphan"
    )


class EngineReportVersion(Base):
    __tablename__ = "engine_report_versions"
    __table_args__ = (
        UniqueConstraint("report_id", "version", name="uq_engine_report_versions_report_version"),
    )

    id = Column(String(36), primary_key=True, default=_uuid)
    report_id = Column(
        String(36),
        ForeignKey("engine_report_definitions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    version = Column(Integer, nullable=False)
    payload_json = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    created_by = Column(String(128), nullable=True)

    report = relationship("EngineReportDefinition", back_populates="versions")


class EngineTransformationDefinition(TimestampMixin, Base):
    __tablename__ = "engine_transformations"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_engine_transformations_tenant_name"),
    )

    id = Column(String(36), primary_key=True, default=_uuid)
    tenant_id = Column(
        String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name = Column(String(256), nullable=False)
    description = Column(Text, nullable=True)
    active_version = Column(Integer, default=1, nullable=False)
    created_by = Column(String(128), nullable=True)

    tenant = relationship("Tenant")
    versions = relationship(
        "EngineTransformationVersion",
        back_populates="transformation",
        cascade="all, delete-orphan",
    )


class EngineTransformationVersion(Base):
    __tablename__ = "engine_transformation_versions"
    __table_args__ = (
        UniqueConstraint(
            "transformation_id",
            "version",
            name="uq_engine_transformation_versions_definition_version",
        ),
    )

    id = Column(String(36), primary_key=True, default=_uuid)
    transformation_id = Column(
        String(36),
        ForeignKey("engine_transformations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    version = Column(Integer, nullable=False)
    payload_json = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    created_by = Column(String(128), nullable=True)

    transformation = relationship("EngineTransformationDefinition", back_populates="versions")


class EngineSegmentDefinition(TimestampMixin, Base):
    __tablename__ = "engine_segments"
    __table_args__ = (UniqueConstraint("tenant_id", "name", name="uq_engine_segments_tenant_name"),)

    id = Column(String(36), primary_key=True, default=_uuid)
    tenant_id = Column(
        String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name = Column(String(256), nullable=False)
    description = Column(Text, nullable=True)
    active_version = Column(Integer, default=1, nullable=False)
    created_by = Column(String(128), nullable=True)

    tenant = relationship("Tenant")
    versions = relationship(
        "EngineSegmentVersion",
        back_populates="segment",
        cascade="all, delete-orphan",
    )


class EngineSegmentVersion(Base):
    __tablename__ = "engine_segment_versions"
    __table_args__ = (
        UniqueConstraint(
            "segment_id",
            "version",
            name="uq_engine_segment_versions_definition_version",
        ),
    )

    id = Column(String(36), primary_key=True, default=_uuid)
    segment_id = Column(
        String(36),
        ForeignKey("engine_segments.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    version = Column(Integer, nullable=False)
    payload_json = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    created_by = Column(String(128), nullable=True)

    segment = relationship("EngineSegmentDefinition", back_populates="versions")


class EngineViewDefinition(TimestampMixin, Base):
    __tablename__ = "engine_views"
    __table_args__ = (UniqueConstraint("tenant_id", "name", name="uq_engine_views_tenant_name"),)

    id = Column(String(36), primary_key=True, default=_uuid)
    tenant_id = Column(
        String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name = Column(String(256), nullable=False)
    description = Column(Text, nullable=True)
    active_version = Column(Integer, default=1, nullable=False)
    created_by = Column(String(128), nullable=True)

    tenant = relationship("Tenant")
    versions = relationship(
        "EngineViewVersion",
        back_populates="view",
        cascade="all, delete-orphan",
    )


class EngineViewVersion(Base):
    __tablename__ = "engine_view_versions"
    __table_args__ = (
        UniqueConstraint(
            "view_id",
            "version",
            name="uq_engine_view_versions_definition_version",
        ),
    )

    id = Column(String(36), primary_key=True, default=_uuid)
    view_id = Column(
        String(36),
        ForeignKey("engine_views.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    version = Column(Integer, nullable=False)
    payload_json = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    created_by = Column(String(128), nullable=True)

    view = relationship("EngineViewDefinition", back_populates="versions")


class EngineRuleSetDefinition(TimestampMixin, Base):
    __tablename__ = "engine_rule_sets"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_engine_rule_sets_tenant_name"),
        Index("ix_engine_rule_sets_tenant_archived", "tenant_id", "is_archived"),
        Index("ix_engine_rule_sets_tenant_active", "tenant_id", "active_version"),
        Index("ix_engine_rule_sets_tenant_published", "tenant_id", "published_version"),
    )

    id = Column(String(36), primary_key=True, default=_uuid)
    tenant_id = Column(
        String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name = Column(String(256), nullable=False)
    description = Column(Text, nullable=True)
    active_version = Column(Integer, default=1, nullable=False)
    published_version = Column(Integer, nullable=True)
    is_archived = Column(Boolean, nullable=False, default=False)
    created_by = Column(String(128), nullable=True)

    tenant = relationship("Tenant")
    versions = relationship(
        "EngineRuleSetVersion",
        back_populates="ruleset",
        cascade="all, delete-orphan",
    )


class EngineRuleSetVersion(Base):
    __tablename__ = "engine_rule_set_versions"
    __table_args__ = (
        CheckConstraint(
            "status IN ('draft', 'validated', 'published', 'archived')",
            name="ck_engine_rule_set_versions_status",
        ),
        UniqueConstraint(
            "ruleset_id", "version", name="uq_engine_rule_set_versions_ruleset_version"
        ),
        Index("ix_engine_rule_set_versions_ruleset_status", "ruleset_id", "status"),
        Index("ix_engine_rule_set_versions_tenant_status", "tenant_id", "status"),
    )

    id = Column(String(36), primary_key=True, default=_uuid)
    ruleset_id = Column(
        String(36),
        ForeignKey("engine_rule_sets.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    tenant_id = Column(
        String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    version = Column(Integer, nullable=False)
    status = Column(String(16), nullable=False, default="draft")
    payload_json = Column(Text, nullable=False)
    validation_errors_json = Column(Text, nullable=True)
    validated_at = Column(DateTime(timezone=True), nullable=True)
    validated_by = Column(String(128), nullable=True)
    published_at = Column(DateTime(timezone=True), nullable=True)
    published_by = Column(String(128), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    created_by = Column(String(128), nullable=True)

    ruleset = relationship("EngineRuleSetDefinition", back_populates="versions")
    tenant = relationship("Tenant")
    blocks = relationship(
        "EngineRuleBlock",
        back_populates="ruleset_version",
        cascade="all, delete-orphan",
    )


class EngineRuleBlock(Base):
    __tablename__ = "engine_rule_blocks"
    __table_args__ = (
        CheckConstraint(
            "block_type IN ('special', 'primary', 'flags')",
            name="ck_engine_rule_blocks_type",
        ),
        CheckConstraint(
            "execution_mode IN ('bypass', 'first_match_wins', 'additive')",
            name="ck_engine_rule_blocks_mode",
        ),
        UniqueConstraint(
            "ruleset_version_id",
            "block_type",
            name="uq_engine_rule_blocks_version_type",
        ),
    )

    id = Column(String(36), primary_key=True, default=_uuid)
    ruleset_version_id = Column(
        String(36),
        ForeignKey("engine_rule_set_versions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    block_type = Column(String(16), nullable=False)
    execution_mode = Column(String(32), nullable=False)
    order_index = Column(Integer, nullable=False, default=0)

    ruleset_version = relationship("EngineRuleSetVersion", back_populates="blocks")
    entries = relationship(
        "EngineRuleEntry",
        back_populates="rule_block",
        cascade="all, delete-orphan",
    )


class EngineRuleEntry(Base):
    __tablename__ = "engine_rule_entries"
    __table_args__ = (
        CheckConstraint("priority >= 0", name="ck_engine_rule_entries_priority_non_negative"),
        Index("ix_engine_rule_entries_block_priority", "rule_block_id", "priority"),
    )

    id = Column(String(36), primary_key=True, default=_uuid)
    rule_block_id = Column(
        String(36),
        ForeignKey("engine_rule_blocks.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    rule_key = Column(String(128), nullable=True)
    priority = Column(Integer, nullable=False, default=0)
    condition_json = Column(Text, nullable=False)
    output_json = Column(Text, nullable=False)
    description = Column(Text, nullable=True)
    order_index = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    created_by = Column(String(128), nullable=True)

    rule_block = relationship("EngineRuleBlock", back_populates="entries")


class EngineClassificationMode(TimestampMixin, Base):
    __tablename__ = "engine_classification_modes"
    __table_args__ = (
        CheckConstraint(
            "mode IN ('legacy', 'shadow', 'declarative')",
            name="ck_engine_classification_modes_mode",
        ),
        UniqueConstraint("tenant_id", name="uq_engine_classification_modes_tenant"),
    )

    id = Column(String(36), primary_key=True, default=_uuid)
    tenant_id = Column(
        String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    mode = Column(String(16), nullable=False, default="legacy")
    ruleset_name = Column(String(256), nullable=True)
    updated_by = Column(String(128), nullable=True)

    tenant = relationship("Tenant")


class EngineClassificationMigration(TimestampMixin, Base):
    __tablename__ = "engine_classification_migrations"
    __table_args__ = (
        CheckConstraint(
            "phase IN ('A', 'B', 'C', 'D')",
            name="ck_engine_classification_migrations_phase",
        ),
        UniqueConstraint("tenant_id", name="uq_engine_classification_migrations_tenant"),
    )

    id = Column(String(36), primary_key=True, default=_uuid)
    tenant_id = Column(
        String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    ruleset_id = Column(
        String(36),
        ForeignKey("engine_rule_sets.id", ondelete="SET NULL"),
        nullable=True,
    )
    ruleset_name = Column(String(256), nullable=True)
    baseline_version = Column(Integer, nullable=True)
    phase = Column(String(8), nullable=False, default="A")
    parity_target_percent = Column(Float, nullable=False, default=99.9)
    last_parity_percent = Column(Float, nullable=True)
    last_parity_passed = Column(Boolean, nullable=True)
    last_dataset_version_id = Column(
        String(36),
        ForeignKey("dataset_versions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    last_run_id = Column(
        String(36),
        ForeignKey("engine_runs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    updated_by = Column(String(128), nullable=True)

    tenant = relationship("Tenant")
    ruleset = relationship("EngineRuleSetDefinition")
    dataset_version = relationship("DatasetVersion")
    run = relationship("EngineRun")


class EngineClassificationDivergence(Base):
    __tablename__ = "engine_classification_divergences"
    __table_args__ = (
        Index(
            "ix_engine_classification_divergences_tenant_created_at",
            "tenant_id",
            "created_at",
        ),
        Index(
            "ix_engine_classification_divergences_tenant_dataset",
            "tenant_id",
            "dataset_version_id",
        ),
    )

    id = Column(String(36), primary_key=True, default=_uuid)
    tenant_id = Column(
        String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    dataset_version_id = Column(
        String(36),
        ForeignKey("dataset_versions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    run_id = Column(
        String(36),
        ForeignKey("engine_runs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    ruleset_name = Column(String(256), nullable=True)
    machine_id = Column(String(256), nullable=True)
    hostname = Column(String(256), nullable=True)
    legacy_primary_status = Column(String(64), nullable=True)
    legacy_primary_status_label = Column(String(256), nullable=True)
    legacy_flags_json = Column(Text, nullable=True)
    declarative_primary_status = Column(String(64), nullable=True)
    declarative_primary_status_label = Column(String(256), nullable=True)
    declarative_flags_json = Column(Text, nullable=True)
    diff_json = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    tenant = relationship("Tenant")
    dataset_version = relationship("DatasetVersion")
    run = relationship("EngineRun")


class EngineRun(TimestampMixin, Base):
    __tablename__ = "engine_runs"

    id = Column(String(36), primary_key=True, default=_uuid)
    tenant_id = Column(
        String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    dataset_version_id = Column(
        String(36),
        ForeignKey("dataset_versions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    report_version_id = Column(
        String(36), ForeignKey("engine_report_versions.id", ondelete="SET NULL"), nullable=True
    )
    run_type = Column(String(32), nullable=False)
    status = Column(String(32), nullable=False, default="pending")
    started_at = Column(DateTime(timezone=True), nullable=True)
    ended_at = Column(DateTime(timezone=True), nullable=True)
    metrics_json = Column(Text, nullable=True)
    error_truncated = Column(Text, nullable=True)

    tenant = relationship("Tenant")
    dataset_version = relationship("DatasetVersion")
    report_version = relationship("EngineReportVersion")
