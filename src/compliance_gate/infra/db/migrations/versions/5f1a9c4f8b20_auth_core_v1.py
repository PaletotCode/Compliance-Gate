"""auth_core_v1

Revision ID: 5f1a9c4f8b20
Revises: 186c72e410d9
Create Date: 2026-03-07 14:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "5f1a9c4f8b20"
down_revision: Union[str, Sequence[str], None] = "186c72e410d9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # tenants
    op.add_column("tenants", sa.Column("name", sa.String(length=256), nullable=True))
    op.execute(
        """
        UPDATE tenants
        SET name = COALESCE(NULLIF(display_name, ''), NULLIF(slug, ''), id)
        WHERE name IS NULL OR name = ''
        """
    )
    op.alter_column("tenants", "name", existing_type=sa.String(length=256), nullable=False)

    # users
    op.alter_column("users", "email", existing_type=sa.String(length=255), nullable=True)
    op.alter_column("users", "name", existing_type=sa.String(length=255), nullable=True)

    op.add_column("users", sa.Column("username", sa.String(length=128), nullable=True))
    op.add_column("users", sa.Column("password_hash", sa.String(length=255), nullable=True))
    op.add_column("users", sa.Column("role", sa.String(length=32), nullable=True))
    op.add_column(
        "users",
        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
    )
    op.add_column(
        "users",
        sa.Column(
            "mfa_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.add_column("users", sa.Column("mfa_secret_protected", sa.Text(), nullable=True))
    op.add_column(
        "users",
        sa.Column(
            "require_password_change",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )

    op.execute(
        """
        UPDATE users
        SET username = COALESCE(
            NULLIF(username, ''),
            NULLIF(regexp_replace(lower(COALESCE(email, '')), '@.*$', ''), ''),
            id
        )
        """
    )
    op.execute(
        """
        WITH ranked AS (
            SELECT id, tenant_id, username,
                   ROW_NUMBER() OVER (PARTITION BY tenant_id, username ORDER BY created_at, id) AS rn
            FROM users
        )
        UPDATE users AS u
        SET username = u.username || '_' || substring(u.id from 1 for 6)
        FROM ranked
        WHERE ranked.id = u.id
          AND ranked.rn > 1
        """
    )
    op.execute(
        """
        UPDATE users
        SET
            password_hash = COALESCE(NULLIF(password_hash, ''), 'BOOTSTRAP_RESET_REQUIRED'),
            role = COALESCE(NULLIF(role, ''), 'DIRECTOR')
        """
    )

    op.alter_column("users", "username", existing_type=sa.String(length=128), nullable=False)
    op.alter_column("users", "password_hash", existing_type=sa.String(length=255), nullable=False)
    op.alter_column("users", "role", existing_type=sa.String(length=32), nullable=False)

    op.create_unique_constraint("uq_users_tenant_username", "users", ["tenant_id", "username"])
    op.create_index("ix_users_tenant_username", "users", ["tenant_id", "username"], unique=False)
    op.create_check_constraint(
        "ck_users_role",
        "users",
        "role IN ('TI_ADMIN', 'DIRECTOR', 'TI_OPERATOR', 'AUDITOR')",
    )

    # recovery_codes
    op.create_table(
        "recovery_codes",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("user_id", sa.String(length=36), nullable=False),
        sa.Column("code_hash", sa.String(length=128), nullable=False),
        sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_recovery_codes_user_id", "recovery_codes", ["user_id"], unique=False)

    # auth_audit
    op.create_table(
        "auth_audit",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("tenant_id", sa.String(length=36), nullable=True),
        sa.Column("user_id", sa.String(length=36), nullable=True),
        sa.Column("action", sa.String(length=64), nullable=False),
        sa.Column("meta_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_auth_audit_tenant_id", "auth_audit", ["tenant_id"], unique=False)
    op.create_index("ix_auth_audit_user_id", "auth_audit", ["user_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_auth_audit_user_id", table_name="auth_audit")
    op.drop_index("ix_auth_audit_tenant_id", table_name="auth_audit")
    op.drop_table("auth_audit")

    op.drop_index("ix_recovery_codes_user_id", table_name="recovery_codes")
    op.drop_table("recovery_codes")

    op.drop_constraint("ck_users_role", "users", type_="check")
    op.drop_index("ix_users_tenant_username", table_name="users")
    op.drop_constraint("uq_users_tenant_username", "users", type_="unique")

    op.drop_column("users", "require_password_change")
    op.drop_column("users", "mfa_secret_protected")
    op.drop_column("users", "mfa_enabled")
    op.drop_column("users", "is_active")
    op.drop_column("users", "role")
    op.drop_column("users", "password_hash")
    op.drop_column("users", "username")

    op.alter_column("users", "name", existing_type=sa.String(length=255), nullable=False)
    op.alter_column("users", "email", existing_type=sa.String(length=255), nullable=False)

    op.drop_column("tenants", "name")
