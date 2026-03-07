from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from compliance_gate.authentication.models import User
from compliance_gate.authentication.rate_limit.limiter import auth_limiter
from compliance_gate.authentication.schemas import MfaConfirmResponse, MfaSetupResponse
from compliance_gate.authentication.security import recovery, totp
from compliance_gate.authentication.storage import repo


@dataclass(slots=True)
class MFAServiceError(Exception):
    message: str
    status_code: int


class MFAService:
    @staticmethod
    def setup_mfa(user: User) -> MfaSetupResponse:
        secret = totp.generate_secret()
        auth_limiter.store_pending_mfa_secret(user.id, secret)

        otpauth_url = totp.build_otpauth_url(secret, user.username, user.tenant_id)
        qr_png_b64 = totp.qr_code_base64_png(otpauth_url)
        return MfaSetupResponse(
            otpauth_url=otpauth_url,
            qr_code_base64_png=qr_png_b64,
            instructions=(
                "Abra o Microsoft Authenticator, escolha adicionar conta, "
                "escaneie o QR Code e confirme com o código de 6 dígitos."
            ),
        )

    @staticmethod
    def confirm_mfa(db: Session, *, user: User, totp_code: str) -> MfaConfirmResponse:
        pending_secret = auth_limiter.get_pending_mfa_secret(user.id)
        if not pending_secret:
            raise MFAServiceError("MFA setup not found or expired", 400)

        if not totp.verify_totp(pending_secret, totp_code):
            raise MFAServiceError("invalid TOTP code", 401)

        protected = totp.protect_secret(pending_secret)
        repo.set_user_mfa_secret(db, user=user, protected_secret=protected)

        recovery_codes = recovery.generate_recovery_codes()
        repo.replace_recovery_codes(
            db,
            user_id=user.id,
            code_hashes=[recovery.hash_recovery_code(code) for code in recovery_codes],
        )
        repo.append_auth_audit(
            db,
            tenant_id=user.tenant_id,
            user_id=user.id,
            action="MFA_SETUP",
            meta={"recovery_codes_count": len(recovery_codes)},
        )
        db.commit()
        auth_limiter.clear_pending_mfa_secret(user.id)

        return MfaConfirmResponse(recovery_codes=recovery_codes)

    @staticmethod
    def verify_user_totp(user: User, totp_code: str) -> bool:
        if not user.mfa_enabled or not user.mfa_secret_protected:
            return False
        secret = totp.reveal_secret(user.mfa_secret_protected)
        return totp.verify_totp(secret, totp_code)
