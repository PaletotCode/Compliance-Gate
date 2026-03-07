"""engine_hardening_v1

Revision ID: 9c2de6a84f1b
Revises: 5f1a9c4f8b20
Create Date: 2026-03-07 14:20:00.000000

"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9c2de6a84f1b"
down_revision: str | Sequence[str] | None = "5f1a9c4f8b20"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("engine_artifacts", sa.Column("artifact_name", sa.String(length=128), nullable=True))
    op.add_column("engine_artifacts", sa.Column("schema_json", sa.Text(), nullable=True))

    op.execute(
        """
        UPDATE engine_artifacts
        SET artifact_name = COALESCE(NULLIF(artifact_name, ''), 'machines_final')
        """
    )
    op.alter_column("engine_artifacts", "artifact_name", existing_type=sa.String(length=128), nullable=False)

    op.create_index("ix_engine_artifacts_artifact_name", "engine_artifacts", ["artifact_name"], unique=False)
    op.create_unique_constraint(
        "uq_engine_artifacts_key",
        "engine_artifacts",
        ["tenant_id", "dataset_version_id", "artifact_type", "artifact_name"],
    )

    op.create_unique_constraint(
        "uq_engine_report_definitions_tenant_name",
        "engine_report_definitions",
        ["tenant_id", "name"],
    )
    op.create_unique_constraint(
        "uq_engine_report_versions_report_version",
        "engine_report_versions",
        ["report_id", "version"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_engine_report_versions_report_version", "engine_report_versions", type_="unique")
    op.drop_constraint("uq_engine_report_definitions_tenant_name", "engine_report_definitions", type_="unique")

    op.drop_constraint("uq_engine_artifacts_key", "engine_artifacts", type_="unique")
    op.drop_index("ix_engine_artifacts_artifact_name", table_name="engine_artifacts")

    op.drop_column("engine_artifacts", "schema_json")
    op.drop_column("engine_artifacts", "artifact_name")
