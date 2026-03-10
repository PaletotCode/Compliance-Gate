"""engine_rulesets_v2

Revision ID: c4a6d91f3b7e
Revises: 7d3c35f4f4d1
Create Date: 2026-03-10 10:30:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c4a6d91f3b7e"
down_revision: str | Sequence[str] | None = "7d3c35f4f4d1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "engine_rule_sets",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("name", sa.String(length=256), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("active_version", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("published_version", sa.Integer(), nullable=True),
        sa.Column("is_archived", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("created_by", sa.String(length=128), nullable=True),
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
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tenant_id", "name", name="uq_engine_rule_sets_tenant_name"),
    )
    op.create_index(op.f("ix_engine_rule_sets_tenant_id"), "engine_rule_sets", ["tenant_id"], unique=False)
    op.create_index(
        "ix_engine_rule_sets_tenant_archived",
        "engine_rule_sets",
        ["tenant_id", "is_archived"],
        unique=False,
    )
    op.create_index(
        "ix_engine_rule_sets_tenant_active",
        "engine_rule_sets",
        ["tenant_id", "active_version"],
        unique=False,
    )
    op.create_index(
        "ix_engine_rule_sets_tenant_published",
        "engine_rule_sets",
        ["tenant_id", "published_version"],
        unique=False,
    )

    op.create_table(
        "engine_rule_set_versions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("ruleset_id", sa.String(length=36), nullable=False),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default=sa.text("'draft'")),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("validation_errors_json", sa.Text(), nullable=True),
        sa.Column("validated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("validated_by", sa.String(length=128), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("published_by", sa.String(length=128), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("created_by", sa.String(length=128), nullable=True),
        sa.CheckConstraint(
            "status IN ('draft', 'validated', 'published', 'archived')",
            name="ck_engine_rule_set_versions_status",
        ),
        sa.ForeignKeyConstraint(["ruleset_id"], ["engine_rule_sets.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ruleset_id", "version", name="uq_engine_rule_set_versions_ruleset_version"),
    )
    op.create_index(
        op.f("ix_engine_rule_set_versions_ruleset_id"),
        "engine_rule_set_versions",
        ["ruleset_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_engine_rule_set_versions_tenant_id"),
        "engine_rule_set_versions",
        ["tenant_id"],
        unique=False,
    )
    op.create_index(
        "ix_engine_rule_set_versions_ruleset_status",
        "engine_rule_set_versions",
        ["ruleset_id", "status"],
        unique=False,
    )
    op.create_index(
        "ix_engine_rule_set_versions_tenant_status",
        "engine_rule_set_versions",
        ["tenant_id", "status"],
        unique=False,
    )

    op.create_table(
        "engine_rule_blocks",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("ruleset_version_id", sa.String(length=36), nullable=False),
        sa.Column("block_type", sa.String(length=16), nullable=False),
        sa.Column("execution_mode", sa.String(length=32), nullable=False),
        sa.Column("order_index", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.CheckConstraint(
            "block_type IN ('special', 'primary', 'flags')",
            name="ck_engine_rule_blocks_type",
        ),
        sa.CheckConstraint(
            "execution_mode IN ('bypass', 'first_match_wins', 'additive')",
            name="ck_engine_rule_blocks_mode",
        ),
        sa.ForeignKeyConstraint(
            ["ruleset_version_id"],
            ["engine_rule_set_versions.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "ruleset_version_id",
            "block_type",
            name="uq_engine_rule_blocks_version_type",
        ),
    )
    op.create_index(
        op.f("ix_engine_rule_blocks_ruleset_version_id"),
        "engine_rule_blocks",
        ["ruleset_version_id"],
        unique=False,
    )

    op.create_table(
        "engine_rule_entries",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("rule_block_id", sa.String(length=36), nullable=False),
        sa.Column("rule_key", sa.String(length=128), nullable=True),
        sa.Column("priority", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("condition_json", sa.Text(), nullable=False),
        sa.Column("output_json", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("order_index", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("created_by", sa.String(length=128), nullable=True),
        sa.CheckConstraint("priority >= 0", name="ck_engine_rule_entries_priority_non_negative"),
        sa.ForeignKeyConstraint(["rule_block_id"], ["engine_rule_blocks.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_engine_rule_entries_rule_block_id"),
        "engine_rule_entries",
        ["rule_block_id"],
        unique=False,
    )
    op.create_index(
        "ix_engine_rule_entries_block_priority",
        "engine_rule_entries",
        ["rule_block_id", "priority"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_engine_rule_entries_block_priority", table_name="engine_rule_entries")
    op.drop_index(op.f("ix_engine_rule_entries_rule_block_id"), table_name="engine_rule_entries")
    op.drop_table("engine_rule_entries")

    op.drop_index(op.f("ix_engine_rule_blocks_ruleset_version_id"), table_name="engine_rule_blocks")
    op.drop_table("engine_rule_blocks")

    op.drop_index("ix_engine_rule_set_versions_tenant_status", table_name="engine_rule_set_versions")
    op.drop_index("ix_engine_rule_set_versions_ruleset_status", table_name="engine_rule_set_versions")
    op.drop_index(op.f("ix_engine_rule_set_versions_tenant_id"), table_name="engine_rule_set_versions")
    op.drop_index(op.f("ix_engine_rule_set_versions_ruleset_id"), table_name="engine_rule_set_versions")
    op.drop_table("engine_rule_set_versions")

    op.drop_index("ix_engine_rule_sets_tenant_published", table_name="engine_rule_sets")
    op.drop_index("ix_engine_rule_sets_tenant_active", table_name="engine_rule_sets")
    op.drop_index("ix_engine_rule_sets_tenant_archived", table_name="engine_rule_sets")
    op.drop_index(op.f("ix_engine_rule_sets_tenant_id"), table_name="engine_rule_sets")
    op.drop_table("engine_rule_sets")
