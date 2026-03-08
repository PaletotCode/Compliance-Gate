from __future__ import annotations

from types import SimpleNamespace

from fastapi.testclient import TestClient

from compliance_gate.authentication.config import auth_settings
from compliance_gate.authentication.http import routes as auth_routes
from compliance_gate.authentication.http.dependencies import get_current_user
from compliance_gate.authentication.services.auth_service import LoginSuccessContext
from compliance_gate.main import app


def test_login_sets_http_only_cookie_and_hides_token(monkeypatch) -> None:
    fake_user = SimpleNamespace(
        id="user-1",
        tenant_id="default",
        username="admin",
        role="TI_ADMIN",
        is_active=True,
        mfa_enabled=False,
        require_password_change=False,
    )

    monkeypatch.setattr(auth_routes.UsersService, "ensure_bootstrap_admin", lambda _db: None)
    monkeypatch.setattr(
        auth_routes.AuthService,
        "authenticate",
        lambda *_args, **_kwargs: LoginSuccessContext(
            access_token="jwt-cookie-token",
            expires_in=2700,
            user=fake_user,
        ),
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/auth/login",
            json={"username": "admin", "password": "secret"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert "access_token" not in payload
    assert "token_type" not in payload
    assert payload["user"]["username"] == "admin"
    assert payload["expires_in"] == 2700

    auth_cookie_value = response.cookies.get(auth_settings.auth_cookie_name)
    csrf_cookie_value = response.cookies.get(auth_settings.csrf_cookie_name)
    assert auth_cookie_value == "jwt-cookie-token"
    assert csrf_cookie_value

    set_cookie_headers = response.headers.get_list("set-cookie")
    auth_set_cookie = next(
        header for header in set_cookie_headers if header.startswith(f"{auth_settings.auth_cookie_name}=")
    )
    csrf_set_cookie = next(
        header for header in set_cookie_headers if header.startswith(f"{auth_settings.csrf_cookie_name}=")
    )
    assert "HttpOnly" in auth_set_cookie
    assert "HttpOnly" not in csrf_set_cookie


def test_csrf_blocks_state_change_without_matching_header() -> None:
    fake_user = SimpleNamespace(
        id="user-1",
        tenant_id="default",
        username="admin",
        role="TI_ADMIN",
        is_active=True,
        mfa_enabled=False,
        require_password_change=False,
    )

    app.dependency_overrides[get_current_user] = lambda: fake_user

    try:
        with TestClient(app) as client:
            client.cookies.set(
                auth_settings.csrf_cookie_name,
                "csrf-123",
                path=auth_settings.auth_cookie_path,
            )

            blocked = client.post("/api/v1/auth/logout")
            assert blocked.status_code == 403

            allowed = client.post(
                "/api/v1/auth/logout",
                headers={auth_settings.csrf_header_name: "csrf-123"},
            )
            assert allowed.status_code == 200
    finally:
        app.dependency_overrides.pop(get_current_user, None)
