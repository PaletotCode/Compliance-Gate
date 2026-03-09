"""workspace_upload_sessions

Revision ID: b3f8f2a1c7de
Revises: 9c2de6a84f1b
Create Date: 2026-03-08 15:10:00.000000

"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b3f8f2a1c7de"
down_revision: str | Sequence[str] | None = "9c2de6a84f1b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "workspace_upload_sessions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("created_by", sa.String(length=36), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("root_path", sa.Text(), nullable=False),
        sa.Column("total_files", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("total_bytes", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("source_manifest", sa.Text(), nullable=True),
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
        sa.ForeignKeyConstraint(["created_by"], ["users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_workspace_upload_sessions_tenant_id"),
        "workspace_upload_sessions",
        ["tenant_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_workspace_upload_sessions_created_by"),
        "workspace_upload_sessions",
        ["created_by"],
        unique=False,
    )

    op.create_table(
        "workspace_upload_files",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("session_id", sa.String(length=36), nullable=False),
        sa.Column("source", sa.String(length=16), nullable=False),
        sa.Column("original_filename", sa.String(length=256), nullable=False),
        sa.Column("stored_filename", sa.String(length=256), nullable=False),
        sa.Column("checksum_sha256", sa.String(length=64), nullable=True),
        sa.Column("file_size_bytes", sa.Integer(), nullable=True),
        sa.Column("detected_encoding", sa.String(length=32), nullable=True),
        sa.Column("validation_warnings", sa.Text(), nullable=True),
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
        sa.ForeignKeyConstraint(
            ["session_id"],
            ["workspace_upload_sessions.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "session_id",
            "source",
            name="uq_workspace_upload_files_session_source",
        ),
    )
    op.create_index(
        op.f("ix_workspace_upload_files_session_id"),
        "workspace_upload_files",
        ["session_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_workspace_upload_files_session_id"), table_name="workspace_upload_files")
    op.drop_table("workspace_upload_files")

    op.drop_index(op.f("ix_workspace_upload_sessions_created_by"), table_name="workspace_upload_sessions")
    op.drop_index(op.f("ix_workspace_upload_sessions_tenant_id"), table_name="workspace_upload_sessions")
    op.drop_table("workspace_upload_sessions")
