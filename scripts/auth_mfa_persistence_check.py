from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass

import pyotp
import requests


AUTH_COOKIE_NAME = os.environ.get("AUTH_COOKIE_NAME", "cg_access")
CSRF_COOKIE_NAME = os.environ.get("CSRF_COOKIE_NAME", "cg_csrf")
CSRF_HEADER_NAME = os.environ.get("CSRF_HEADER_NAME", "X-CSRF-Token")


@dataclass(slots=True)
class HttpSession:
    base_url: str
    session: requests.Session

    def request(self, method: str, path: str, payload: dict | None = None, attach_csrf: bool = True) -> requests.Response:
        headers: dict[str, str] = {}
        if attach_csrf and method.upper() in {"POST", "PUT", "PATCH", "DELETE"}:
            csrf_token = self.session.cookies.get(CSRF_COOKIE_NAME)
            if csrf_token:
                headers[CSRF_HEADER_NAME] = csrf_token
        response = self.session.request(
            method.upper(),
            f"{self.base_url}{path}",
            json=payload,
            headers=headers,
            timeout=30,
        )
        return response


def normalize_base_url(base_url: str) -> str:
    return base_url[:-1] if base_url.endswith("/") else base_url


def login(http: HttpSession, username: str, password: str, *, totp_code: str | None = None, challenge_id: str | None = None) -> dict:
    body: dict[str, str] = {"username": username, "password": password}
    if totp_code:
        body["totp_code"] = totp_code
    if challenge_id:
        body["challenge_id"] = challenge_id
    response = http.request("POST", "/api/v1/auth/login", body, attach_csrf=False)
    if response.status_code != 200:
        raise RuntimeError(f"login failed ({response.status_code}): {response.text}")
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("login payload is not a dict")
    return payload


def restart_api_container() -> None:
    subprocess.run(["docker", "compose", "restart", "api"], check=True)
    for _ in range(60):
        try:
            health = requests.get("http://localhost:8000/health", timeout=2)
            if health.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(1)
    raise RuntimeError("api did not become healthy after restart")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate MFA persistence for a user across API restart.")
    parser.add_argument("--base-url", default=os.environ.get("API_BASE_URL", "http://localhost:8000"))
    parser.add_argument("--admin-username", default=os.environ.get("AUTH_BOOTSTRAP_ADMIN_USERNAME", "admin"))
    parser.add_argument("--admin-password", default=os.environ.get("AUTH_BOOTSTRAP_ADMIN_PASSWORD", "Admin1234"))
    parser.add_argument("--test-password", default="Persist123!")
    parser.add_argument("--restart-api", action="store_true")
    args = parser.parse_args()

    base_url = normalize_base_url(args.base_url)
    user_name = f"mfa_persist_{int(time.time())}"

    admin_http = HttpSession(base_url=base_url, session=requests.Session())
    admin_login = login(admin_http, args.admin_username, args.admin_password)
    if admin_login.get("mfa_required"):
        raise RuntimeError("admin login returned MFA challenge; use non-MFA admin for this check")
    if not admin_http.session.cookies.get(AUTH_COOKIE_NAME):
        raise RuntimeError(f"missing auth cookie {AUTH_COOKIE_NAME} after admin login")

    create_response = admin_http.request(
        "POST",
        "/api/v1/auth/users",
        {"username": user_name, "password": args.test_password, "role": "DIRECTOR"},
    )
    if create_response.status_code not in (201, 400):
        raise RuntimeError(f"failed creating check user ({create_response.status_code}): {create_response.text}")

    user_http = HttpSession(base_url=base_url, session=requests.Session())
    first_login = login(user_http, user_name, args.test_password)
    if first_login.get("mfa_required"):
        raise RuntimeError("new user should not require MFA before setup")

    setup_response = user_http.request("POST", "/api/v1/auth/mfa/setup", {})
    if setup_response.status_code != 200:
        raise RuntimeError(f"mfa setup failed ({setup_response.status_code}): {setup_response.text}")
    setup_payload = setup_response.json()
    otpauth_url = setup_payload.get("otpauth_url")
    if not isinstance(otpauth_url, str) or not otpauth_url:
        raise RuntimeError("mfa setup did not return otpauth_url")

    totp = pyotp.parse_uri(otpauth_url)
    confirm_response = user_http.request(
        "POST",
        "/api/v1/auth/mfa/confirm",
        {"totp_code": totp.now()},
    )
    if confirm_response.status_code != 200:
        raise RuntimeError(f"mfa confirm failed ({confirm_response.status_code}): {confirm_response.text}")

    challenge_before_restart = login(HttpSession(base_url=base_url, session=requests.Session()), user_name, args.test_password)
    if not challenge_before_restart.get("mfa_required"):
        raise RuntimeError("expected MFA challenge after confirmation")

    challenge_after_restart = None
    if args.restart_api:
        restart_api_container()
        challenge_after_restart = login(HttpSession(base_url=base_url, session=requests.Session()), user_name, args.test_password)
        if not challenge_after_restart.get("mfa_required"):
            raise RuntimeError("MFA was not persisted after API restart")

    print(
        json.dumps(
            {
                "status": "ok",
                "test_user": user_name,
                "mfa_challenge_before_restart": bool(challenge_before_restart.get("mfa_required")),
                "mfa_challenge_after_restart": bool(challenge_after_restart.get("mfa_required")) if challenge_after_restart else None,
            }
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[auth_mfa_persistence_check] {exc}", file=sys.stderr)
        raise SystemExit(1)
