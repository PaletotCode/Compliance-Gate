"""engine_classification_runtime_v2

Revision ID: e1f8c9b2d0aa
Revises: c4a6d91f3b7e
Create Date: 2026-03-10 13:20:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e1f8c9b2d0aa"
down_revision: str | Sequence[str] | None = "c4a6d91f3b7e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "engine_classification_modes",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False, server_default=sa.text("'legacy'")),
        sa.Column("ruleset_name", sa.String(length=256), nullable=True),
        sa.Column("updated_by", sa.String(length=128), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.CheckConstraint(
            "mode IN ('legacy', 'shadow', 'declarative')",
            name="ck_engine_classification_modes_mode",
        ),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tenant_id", name="uq_engine_classification_modes_tenant"),
    )
    op.create_index(
        op.f("ix_engine_classification_modes_tenant_id"),
        "engine_classification_modes",
        ["tenant_id"],
        unique=False,
    )

    op.create_table(
        "engine_classification_divergences",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("dataset_version_id", sa.String(length=36), nullable=True),
        sa.Column("run_id", sa.String(length=36), nullable=True),
        sa.Column("ruleset_name", sa.String(length=256), nullable=True),
        sa.Column("machine_id", sa.String(length=256), nullable=True),
        sa.Column("hostname", sa.String(length=256), nullable=True),
        sa.Column("legacy_primary_status", sa.String(length=64), nullable=True),
        sa.Column("legacy_primary_status_label", sa.String(length=256), nullable=True),
        sa.Column("legacy_flags_json", sa.Text(), nullable=True),
        sa.Column("declarative_primary_status", sa.String(length=64), nullable=True),
        sa.Column("declarative_primary_status_label", sa.String(length=256), nullable=True),
        sa.Column("declarative_flags_json", sa.Text(), nullable=True),
        sa.Column("diff_json", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["dataset_version_id"],
            ["dataset_versions.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["engine_runs.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_engine_classification_divergences_tenant_id"),
        "engine_classification_divergences",
        ["tenant_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_engine_classification_divergences_dataset_version_id"),
        "engine_classification_divergences",
        ["dataset_version_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_engine_classification_divergences_run_id"),
        "engine_classification_divergences",
        ["run_id"],
        unique=False,
    )
    op.create_index(
        "ix_engine_classification_divergences_tenant_created_at",
        "engine_classification_divergences",
        ["tenant_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_engine_classification_divergences_tenant_dataset",
        "engine_classification_divergences",
        ["tenant_id", "dataset_version_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_engine_classification_divergences_tenant_dataset",
        table_name="engine_classification_divergences",
    )
    op.drop_index(
        "ix_engine_classification_divergences_tenant_created_at",
        table_name="engine_classification_divergences",
    )
    op.drop_index(
        op.f("ix_engine_classification_divergences_run_id"),
        table_name="engine_classification_divergences",
    )
    op.drop_index(
        op.f("ix_engine_classification_divergences_dataset_version_id"),
        table_name="engine_classification_divergences",
    )
    op.drop_index(
        op.f("ix_engine_classification_divergences_tenant_id"),
        table_name="engine_classification_divergences",
    )
    op.drop_table("engine_classification_divergences")

    op.drop_index(op.f("ix_engine_classification_modes_tenant_id"), table_name="engine_classification_modes")
    op.drop_table("engine_classification_modes")
