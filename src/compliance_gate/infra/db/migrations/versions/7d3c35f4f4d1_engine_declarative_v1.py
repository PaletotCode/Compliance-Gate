"""engine_declarative_v1

Revision ID: 7d3c35f4f4d1
Revises: b3f8f2a1c7de
Create Date: 2026-03-09 20:20:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7d3c35f4f4d1"
down_revision: str | Sequence[str] | None = "b3f8f2a1c7de"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "engine_transformations",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("name", sa.String(length=256), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("active_version", sa.Integer(), nullable=False, server_default=sa.text("1")),
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
        sa.UniqueConstraint("tenant_id", "name", name="uq_engine_transformations_tenant_name"),
    )
    op.create_index(
        op.f("ix_engine_transformations_tenant_id"),
        "engine_transformations",
        ["tenant_id"],
        unique=False,
    )

    op.create_table(
        "engine_transformation_versions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("transformation_id", sa.String(length=36), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("created_by", sa.String(length=128), nullable=True),
        sa.ForeignKeyConstraint(
            ["transformation_id"],
            ["engine_transformations.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "transformation_id",
            "version",
            name="uq_engine_transformation_versions_definition_version",
        ),
    )
    op.create_index(
        op.f("ix_engine_transformation_versions_transformation_id"),
        "engine_transformation_versions",
        ["transformation_id"],
        unique=False,
    )

    op.create_table(
        "engine_segments",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("name", sa.String(length=256), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("active_version", sa.Integer(), nullable=False, server_default=sa.text("1")),
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
        sa.UniqueConstraint("tenant_id", "name", name="uq_engine_segments_tenant_name"),
    )
    op.create_index(
        op.f("ix_engine_segments_tenant_id"),
        "engine_segments",
        ["tenant_id"],
        unique=False,
    )

    op.create_table(
        "engine_segment_versions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("segment_id", sa.String(length=36), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("created_by", sa.String(length=128), nullable=True),
        sa.ForeignKeyConstraint(
            ["segment_id"],
            ["engine_segments.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "segment_id",
            "version",
            name="uq_engine_segment_versions_definition_version",
        ),
    )
    op.create_index(
        op.f("ix_engine_segment_versions_segment_id"),
        "engine_segment_versions",
        ["segment_id"],
        unique=False,
    )

    op.create_table(
        "engine_views",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("name", sa.String(length=256), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("active_version", sa.Integer(), nullable=False, server_default=sa.text("1")),
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
        sa.UniqueConstraint("tenant_id", "name", name="uq_engine_views_tenant_name"),
    )
    op.create_index(
        op.f("ix_engine_views_tenant_id"),
        "engine_views",
        ["tenant_id"],
        unique=False,
    )

    op.create_table(
        "engine_view_versions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("view_id", sa.String(length=36), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("created_by", sa.String(length=128), nullable=True),
        sa.ForeignKeyConstraint(
            ["view_id"],
            ["engine_views.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "view_id",
            "version",
            name="uq_engine_view_versions_definition_version",
        ),
    )
    op.create_index(
        op.f("ix_engine_view_versions_view_id"),
        "engine_view_versions",
        ["view_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_engine_view_versions_view_id"), table_name="engine_view_versions")
    op.drop_table("engine_view_versions")

    op.drop_index(op.f("ix_engine_views_tenant_id"), table_name="engine_views")
    op.drop_table("engine_views")

    op.drop_index(op.f("ix_engine_segment_versions_segment_id"), table_name="engine_segment_versions")
    op.drop_table("engine_segment_versions")

    op.drop_index(op.f("ix_engine_segments_tenant_id"), table_name="engine_segments")
    op.drop_table("engine_segments")

    op.drop_index(
        op.f("ix_engine_transformation_versions_transformation_id"),
        table_name="engine_transformation_versions",
    )
    op.drop_table("engine_transformation_versions")

    op.drop_index(op.f("ix_engine_transformations_tenant_id"), table_name="engine_transformations")
    op.drop_table("engine_transformations")

