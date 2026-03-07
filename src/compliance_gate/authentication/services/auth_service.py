from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass

from sqlalchemy.orm import Session

from compliance_gate.authentication.models import Role, User
from compliance_gate.authentication.rate_limit.limiter import auth_limiter
from compliance_gate.authentication.schemas import (
    LoginChallengeResponse,
    LoginSuccessResponse,
)
from compliance_gate.authentication.security.jwt import create_access_token
from compliance_gate.authentication.security.passwords import verify_password
from compliance_gate.authentication.services.users_service import UsersService
from compliance_gate.authentication.storage import repo
from compliance_gate.authentication.services import mfa_service

log = logging.getLogger(__name__)


@dataclass(slots=True)
class AuthServiceError(Exception):
    message: str
    status_code: int


class AuthService:
    @staticmethod
    def authenticate(
        db: Session,
        *,
        username: str,
        password: str,
        totp_code: str | None,
        challenge_id: str | None,
        ip_address: str | None,
    ) -> LoginChallengeResponse | LoginSuccessResponse:
        normalized_username = username.lower().strip()
        if auth_limiter.is_locked(normalized_username, ip_address):
            raise AuthServiceError("temporarily locked due to failed attempts", 429)

        candidates = repo.get_users_by_username(db, normalized_username)
        if len(candidates) != 1:
            auth_limiter.register_login_failure(normalized_username, ip_address)
            repo.append_auth_audit(
                db,
                tenant_id=None,
                user_id=None,
                action="LOGIN_FAIL",
                meta={"username_hash": repo.hash_identifier(normalized_username), "reason": "not_found_or_ambiguous"},
            )
            db.commit()
            raise AuthServiceError("invalid credentials", 401)

        user = candidates[0]
        if not user.is_active:
            repo.append_auth_audit(
                db,
                tenant_id=user.tenant_id,
                user_id=user.id,
                action="LOGIN_FAIL",
                meta={"reason": "inactive_user"},
            )
            db.commit()
            raise AuthServiceError("invalid credentials", 401)

        if not verify_password(password, user.password_hash):
            auth_limiter.register_login_failure(normalized_username, ip_address)
            repo.append_auth_audit(
                db,
                tenant_id=user.tenant_id,
                user_id=user.id,
                action="LOGIN_FAIL",
                meta={"reason": "bad_password"},
            )
            db.commit()
            raise AuthServiceError("invalid credentials", 401)

        if user.mfa_enabled:
            if not totp_code:
                challenge = str(uuid.uuid4())
                auth_limiter.store_login_challenge(challenge, user.id)
                repo.append_auth_audit(
                    db,
                    tenant_id=user.tenant_id,
                    user_id=user.id,
                    action="LOGIN_MFA_CHALLENGE",
                    meta={"challenge_hash": repo.hash_identifier(challenge)},
                )
                db.commit()
                return LoginChallengeResponse(mfa_required=True, challenge_id=challenge)

            if challenge_id:
                challenge_user = auth_limiter.consume_login_challenge(challenge_id)
                if challenge_user != user.id:
                    auth_limiter.register_login_failure(normalized_username, ip_address)
                    repo.append_auth_audit(
                        db,
                        tenant_id=user.tenant_id,
                        user_id=user.id,
                        action="LOGIN_FAIL",
                        meta={"reason": "invalid_mfa_challenge"},
                    )
                    db.commit()
                    raise AuthServiceError("invalid MFA challenge", 401)

            if not mfa_service.MFAService.verify_user_totp(user, totp_code):
                auth_limiter.register_login_failure(normalized_username, ip_address)
                repo.append_auth_audit(
                    db,
                    tenant_id=user.tenant_id,
                    user_id=user.id,
                    action="LOGIN_FAIL",
                    meta={"reason": "invalid_totp"},
                )
                db.commit()
                raise AuthServiceError("invalid MFA code", 401)

        auth_limiter.clear_login_failures(normalized_username, ip_address)
        token, expires_in = create_access_token(user)
        repo.append_auth_audit(
            db,
            tenant_id=user.tenant_id,
            user_id=user.id,
            action="LOGIN_SUCCESS",
            meta={"role": user.role},
        )
        db.commit()

        return LoginSuccessResponse(
            access_token=token,
            expires_in=expires_in,
            user=UsersService.to_public(user),
        )

    @staticmethod
    def assert_user_has_role(user: User, allowed_roles: tuple[Role, ...]) -> None:
        if Role(user.role) not in allowed_roles:
            raise AuthServiceError("insufficient permissions", 403)
