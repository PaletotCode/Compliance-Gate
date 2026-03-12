from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime

from compliance_gate.authentication.models import RecoveryCode, Role
from compliance_gate.authentication.security.passwords import hash_password
from compliance_gate.authentication.storage import repo
from compliance_gate.infra.db import models as _db_models  # noqa: F401
from compliance_gate.infra.db import models_engine as _db_engine_models  # noqa: F401
from compliance_gate.infra.db import models_profiles as _db_profiles_models  # noqa: F401
from compliance_gate.infra.db.session import SessionLocal


def parse_role(value: str) -> Role:
    try:
        return Role(value.upper())
    except ValueError as exc:
        allowed = ", ".join(role.value for role in Role)
        raise argparse.ArgumentTypeError(f"invalid role '{value}'. allowed: {allowed}") from exc


def main() -> int:
    parser = argparse.ArgumentParser(description="Fully reset an auth user (password + MFA + recovery codes).")
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--role", type=parse_role, default=None)
    parser.add_argument("--tenant-slug", default=os.environ.get("AUTH_BOOTSTRAP_TENANT_SLUG", "default"))
    parser.add_argument("--tenant-name", default=os.environ.get("AUTH_BOOTSTRAP_TENANT_NAME", "Default Tenant"))
    args = parser.parse_args()

    username = args.username.strip().lower()
    if len(username) < 3:
        raise ValueError("username must have at least 3 characters")

    db = SessionLocal()
    try:
        users = repo.get_users_by_username(db, username)
        created = False
        if len(users) > 1:
            raise RuntimeError(f"username '{username}' is ambiguous across tenants")

        if users:
            user = users[0]
            repo.acquire_user_advisory_lock(db, user.id)
            target_role = args.role or Role(user.role)
        else:
            tenant = repo.get_or_create_tenant(
                db,
                tenant_slug=args.tenant_slug,
                tenant_name=args.tenant_name,
            )
            target_role = args.role or Role.DIRECTOR
            user = repo.create_user(
                db,
                tenant_id=tenant.id,
                username=username,
                password_hash=hash_password(args.password),
                role=target_role,
                require_password_change=False,
            )
            created = True

        user.password_hash = hash_password(args.password)
        user.role = target_role.value
        user.is_active = True
        user.require_password_change = False
        user.mfa_enabled = False
        user.mfa_secret_protected = None
        user.updated_at = datetime.now(UTC)

        db.query(RecoveryCode).filter(RecoveryCode.user_id == user.id).delete()

        repo.append_auth_audit(
            db,
            tenant_id=user.tenant_id,
            user_id=user.id,
            action="USER_RESET_FULL",
            meta={"created": created, "role": target_role.value},
        )
        db.commit()

        print(
            json.dumps(
                {
                    "status": "ok",
                    "username": user.username,
                    "user_id": user.id,
                    "tenant_id": user.tenant_id,
                    "created": created,
                    "mfa_enabled": user.mfa_enabled,
                }
            )
        )
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[reset_auth_user] {exc}", file=sys.stderr)
        raise SystemExit(1)
