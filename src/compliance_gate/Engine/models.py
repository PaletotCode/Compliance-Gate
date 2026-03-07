from __future__ import annotations

import uuid

from sqlalchemy import (
    Column,
    DateTime,
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
