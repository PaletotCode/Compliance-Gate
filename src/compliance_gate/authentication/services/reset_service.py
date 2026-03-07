from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from compliance_gate.authentication.models import User
from compliance_gate.authentication.security.passwords import hash_password
from compliance_gate.authentication.security.recovery import hash_recovery_code
from compliance_gate.authentication.services.mfa_service import MFAService
from compliance_gate.authentication.storage import repo


@dataclass(slots=True)
class ResetServiceError(Exception):
    message: str
    status_code: int


class ResetService:
    @staticmethod
    def reset_password(
        db: Session,
        *,
        user: User,
        new_password: str,
        totp_code: str | None,
        recovery_code: str | None,
    ) -> None:
        verified = False

        if totp_code:
            if not user.mfa_enabled:
                raise ResetServiceError("MFA is not enabled for this user", 400)
            verified = MFAService.verify_user_totp(user, totp_code)

        if not verified and recovery_code:
            verified = repo.consume_recovery_code(
                db,
                user_id=user.id,
                code_hash=hash_recovery_code(recovery_code),
            )

        if not verified:
            repo.append_auth_audit(
                db,
                tenant_id=user.tenant_id,
                user_id=user.id,
                action="RESET_PASSWORD_FAIL",
                meta={"reason": "invalid_factor"},
            )
            db.commit()
            raise ResetServiceError("invalid reset factor", 401)

        repo.update_password(
            db,
            user=user,
            password_hash=hash_password(new_password),
            require_password_change=False,
        )
        repo.append_auth_audit(
            db,
            tenant_id=user.tenant_id,
            user_id=user.id,
            action="RESET_PASSWORD_SUCCESS",
            meta={"method": "totp" if totp_code else "recovery_code"},
        )
        db.commit()
