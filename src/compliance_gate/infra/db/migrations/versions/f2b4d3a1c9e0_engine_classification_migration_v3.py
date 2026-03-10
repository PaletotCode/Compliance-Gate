"""engine_classification_migration_v3

Revision ID: f2b4d3a1c9e0
Revises: e1f8c9b2d0aa
Create Date: 2026-03-10 16:10:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f2b4d3a1c9e0"
down_revision: str | Sequence[str] | None = "e1f8c9b2d0aa"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "engine_classification_migrations",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("ruleset_id", sa.String(length=36), nullable=True),
        sa.Column("ruleset_name", sa.String(length=256), nullable=True),
        sa.Column("baseline_version", sa.Integer(), nullable=True),
        sa.Column("phase", sa.String(length=8), nullable=False, server_default=sa.text("'A'")),
        sa.Column(
            "parity_target_percent",
            sa.Float(),
            nullable=False,
            server_default=sa.text("99.9"),
        ),
        sa.Column("last_parity_percent", sa.Float(), nullable=True),
        sa.Column("last_parity_passed", sa.Boolean(), nullable=True),
        sa.Column("last_dataset_version_id", sa.String(length=36), nullable=True),
        sa.Column("last_run_id", sa.String(length=36), nullable=True),
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
            "phase IN ('A', 'B', 'C', 'D')",
            name="ck_engine_classification_migrations_phase",
        ),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["ruleset_id"], ["engine_rule_sets.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["last_dataset_version_id"],
            ["dataset_versions.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(["last_run_id"], ["engine_runs.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tenant_id", name="uq_engine_classification_migrations_tenant"),
    )
    op.create_index(
        op.f("ix_engine_classification_migrations_tenant_id"),
        "engine_classification_migrations",
        ["tenant_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_engine_classification_migrations_last_dataset_version_id"),
        "engine_classification_migrations",
        ["last_dataset_version_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_engine_classification_migrations_last_run_id"),
        "engine_classification_migrations",
        ["last_run_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_engine_classification_migrations_last_run_id"),
        table_name="engine_classification_migrations",
    )
    op.drop_index(
        op.f("ix_engine_classification_migrations_last_dataset_version_id"),
        table_name="engine_classification_migrations",
    )
    op.drop_index(
        op.f("ix_engine_classification_migrations_tenant_id"),
        table_name="engine_classification_migrations",
    )
    op.drop_table("engine_classification_migrations")
