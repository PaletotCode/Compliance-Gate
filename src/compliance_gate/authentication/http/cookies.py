from __future__ import annotations

import secrets
from fastapi import Request, Response

from compliance_gate.authentication.config import auth_settings

SAFE_METHODS = {"GET", "HEAD", "OPTIONS", "TRACE"}


def is_state_changing_method(method: str) -> bool:
    return method.upper() not in SAFE_METHODS


def is_csrf_exempt_path(path: str) -> bool:
    normalized = path.rstrip("/") or "/"
    return normalized == "/api/v1/auth/login"


def generate_csrf_token() -> str:
    return secrets.token_urlsafe(32)


def set_access_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=auth_settings.auth_cookie_name,
        value=token,
        max_age=auth_settings.auth_token_ttl_minutes * 60,
        httponly=True,
        secure=auth_settings.auth_cookie_secure,
        samesite=auth_settings.auth_cookie_samesite,
        path=auth_settings.auth_cookie_path,
    )


def set_csrf_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=auth_settings.csrf_cookie_name,
        value=token,
        max_age=auth_settings.auth_token_ttl_minutes * 60,
        httponly=False,
        secure=auth_settings.auth_cookie_secure,
        samesite=auth_settings.auth_cookie_samesite,
        path=auth_settings.auth_cookie_path,
    )


def ensure_csrf_cookie(request: Request, response: Response) -> None:
    if request.cookies.get(auth_settings.csrf_cookie_name):
        return
    set_csrf_cookie(response, generate_csrf_token())


def clear_auth_cookies(response: Response) -> None:
    response.delete_cookie(
        key=auth_settings.auth_cookie_name,
        path=auth_settings.auth_cookie_path,
        secure=auth_settings.auth_cookie_secure,
        samesite=auth_settings.auth_cookie_samesite,
    )
    response.delete_cookie(
        key=auth_settings.csrf_cookie_name,
        path=auth_settings.auth_cookie_path,
        secure=auth_settings.auth_cookie_secure,
        samesite=auth_settings.auth_cookie_samesite,
    )
