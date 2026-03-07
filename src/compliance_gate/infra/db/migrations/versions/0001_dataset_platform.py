"""
0001_dataset_platform.py — Initial migration for the Data Platform Core.

Creates tables:
  - tenants
  - dataset_versions
  - dataset_files
  - dataset_metrics
  - audit_logs
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── tenants ──────────────────────────────────────────────────────────────
    op.create_table(
        "tenants",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("slug", sa.String(64), nullable=False, unique=True),
        sa.Column("display_name", sa.String(256), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_tenants_slug", "tenants", ["slug"], unique=True)

    # ── dataset_versions ─────────────────────────────────────────────────────
    op.create_table(
        "dataset_versions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "tenant_id",
            sa.String(36),
            sa.ForeignKey("tenants.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source_type", sa.String(32), nullable=False, server_default="machines"),
        sa.Column("status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("profile_id", sa.String(36), nullable=True),
        sa.Column("data_dir", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_dataset_versions_tenant_id", "dataset_versions", ["tenant_id"])

    # ── dataset_files ─────────────────────────────────────────────────────────
    op.create_table(
        "dataset_files",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "version_id",
            sa.String(36),
            sa.ForeignKey("dataset_versions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source", sa.String(16), nullable=False),
        sa.Column("original_filename", sa.String(256), nullable=True),
        sa.Column("resolved_path", sa.Text(), nullable=True),
        sa.Column("checksum_sha256", sa.String(64), nullable=True),
        sa.Column("file_size_bytes", sa.Integer(), nullable=True),
        sa.Column("detected_encoding", sa.String(32), nullable=True),
        sa.Column("detected_delimiter", sa.String(4), nullable=True),
        sa.Column("header_row_index", sa.Integer(), nullable=True),
        sa.Column("detected_headers", sa.Text(), nullable=True),
        sa.Column("rows_read", sa.Integer(), nullable=True),
        sa.Column("rows_valid", sa.Integer(), nullable=True),
        sa.Column("parse_warnings", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_dataset_files_version_id", "dataset_files", ["version_id"])

    # ── dataset_metrics ───────────────────────────────────────────────────────
    op.create_table(
        "dataset_metrics",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "version_id",
            sa.String(36),
            sa.ForeignKey("dataset_versions.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column("total_entries", sa.Integer(), nullable=True),
        sa.Column("from_ad", sa.Integer(), nullable=True),
        sa.Column("from_uem", sa.Integer(), nullable=True),
        sa.Column("from_edr", sa.Integer(), nullable=True),
        sa.Column("match_ad_uem", sa.Integer(), nullable=True),
        sa.Column("match_ad_edr", sa.Integer(), nullable=True),
        sa.Column("asset_matched", sa.Integer(), nullable=True),
        sa.Column("cloned_serials", sa.Integer(), nullable=True),
        sa.Column("rows_read_total", sa.Integer(), nullable=True),
        sa.Column("rows_valid_total", sa.Integer(), nullable=True),
        sa.Column("parse_rate", sa.Float(), nullable=True),
        sa.Column("match_rate", sa.Float(), nullable=True),
        sa.Column("total_elapsed_ms", sa.Float(), nullable=True),
        sa.Column("warnings_count", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_dataset_metrics_version_id",
        "dataset_metrics",
        ["version_id"],
        unique=True,
    )

    # ── audit_logs ────────────────────────────────────────────────────────────
    op.create_table(
        "audit_logs",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "tenant_id",
            sa.String(36),
            sa.ForeignKey("tenants.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "dataset_version_id",
            sa.String(36),
            sa.ForeignKey("dataset_versions.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("actor", sa.String(128), nullable=True),
        sa.Column("action", sa.String(64), nullable=False),
        sa.Column("entity_type", sa.String(64), nullable=True),
        sa.Column("entity_id", sa.String(36), nullable=True),
        sa.Column("details", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_audit_logs_tenant_id", "audit_logs", ["tenant_id"])
    op.create_index("ix_audit_logs_dataset_version_id", "audit_logs", ["dataset_version_id"])


def downgrade() -> None:
    op.drop_table("audit_logs")
    op.drop_table("dataset_metrics")
    op.drop_table("dataset_files")
    op.drop_table("dataset_versions")
    op.drop_table("tenants")
