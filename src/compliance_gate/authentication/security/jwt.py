from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from jose import JWTError, jwt

from compliance_gate.authentication.config import auth_settings
from compliance_gate.authentication.models import User


@dataclass(slots=True)
class TokenPayload:
    sub: str
    tenant_id: str
    role: str
    username: str


def create_access_token(user: User) -> tuple[str, int]:
    expires_delta = timedelta(minutes=auth_settings.auth_token_ttl_minutes)
    now = datetime.now(UTC)
    expire_at = now + expires_delta
    payload = {
        "sub": user.id,
        "tenant_id": user.tenant_id,
        "role": user.role,
        "username": user.username,
        "iss": auth_settings.auth_jwt_issuer,
        "aud": auth_settings.auth_jwt_audience,
        "iat": int(now.timestamp()),
        "exp": int(expire_at.timestamp()),
    }
    token = jwt.encode(
        payload,
        auth_settings.auth_jwt_secret,
        algorithm=auth_settings.auth_jwt_algorithm,
    )
    return token, int(expires_delta.total_seconds())


def decode_access_token(token: str) -> TokenPayload:
    try:
        payload = jwt.decode(
            token,
            auth_settings.auth_jwt_secret,
            algorithms=[auth_settings.auth_jwt_algorithm],
            audience=auth_settings.auth_jwt_audience,
            issuer=auth_settings.auth_jwt_issuer,
        )
    except JWTError as exc:  # includes signature/exp/audience errors
        raise ValueError("invalid token") from exc

    sub = payload.get("sub")
    tenant_id = payload.get("tenant_id")
    role = payload.get("role")
    username = payload.get("username")
    if not sub or not tenant_id or not role or not username:
        raise ValueError("invalid token payload")

    return TokenPayload(sub=sub, tenant_id=tenant_id, role=role, username=username)
