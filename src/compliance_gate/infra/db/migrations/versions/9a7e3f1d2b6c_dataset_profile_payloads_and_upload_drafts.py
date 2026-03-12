"""dataset_profile_payloads_and_upload_drafts

Revision ID: 9a7e3f1d2b6c
Revises: f2b4d3a1c9e0
Create Date: 2026-03-11 12:10:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9a7e3f1d2b6c"
down_revision: str | Sequence[str] | None = "f2b4d3a1c9e0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "dataset_versions",
        sa.Column("used_profile_payloads", sa.Text(), nullable=True),
    )

    op.add_column(
        "workspace_upload_files",
        sa.Column("draft_config_json", sa.Text(), nullable=True),
    )
    op.add_column(
        "workspace_upload_files",
        sa.Column("draft_profile_id", sa.String(length=36), nullable=True),
    )
    op.add_column(
        "workspace_upload_files",
        sa.Column("draft_profile_version", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("workspace_upload_files", "draft_profile_version")
    op.drop_column("workspace_upload_files", "draft_profile_id")
    op.drop_column("workspace_upload_files", "draft_config_json")
    op.drop_column("dataset_versions", "used_profile_payloads")

