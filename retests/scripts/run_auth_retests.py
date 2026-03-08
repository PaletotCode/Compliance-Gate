from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pyotp
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
API_BASE = os.environ.get("API_BASE_URL", "http://localhost:8000")
AUTH_PREFIX = f"{API_BASE}/api/v1/auth"
API_PREFIX = f"{API_BASE}/api/v1"

BOOTSTRAP_ADMIN_USERNAME = os.environ.get("AUTH_BOOTSTRAP_ADMIN_USERNAME", "admin")
BOOTSTRAP_ADMIN_PASSWORD = os.environ.get("AUTH_BOOTSTRAP_ADMIN_PASSWORD", "Admin1234")
DIRECTOR_USERNAME = "director"
DIRECTOR_PASSWORD = "Director123"
ADMIN_NEW_PASSWORD = "Admin9876"

AUTH_COOKIE_NAME = os.environ.get("AUTH_COOKIE_NAME", "cg_access")
CSRF_COOKIE_NAME = os.environ.get("CSRF_COOKIE_NAME", "cg_csrf")
CSRF_HEADER_NAME = os.environ.get("CSRF_HEADER_NAME", "X-CSRF-Token")


class CookieApiClient:
    def __init__(self) -> None:
        self.session = requests.Session()

    def request_json(
        self,
        method: str,
        url: str,
        body: dict,
        *,
        expected_status: int | tuple[int, ...] = 200,
        attach_csrf: bool = True,
    ) -> requests.Response:
        headers: dict[str, str] = {}
        if attach_csrf and method.upper() in {"POST", "PUT", "PATCH", "DELETE"}:
            csrf_token = self.session.cookies.get(CSRF_COOKIE_NAME)
            if csrf_token:
                headers[CSRF_HEADER_NAME] = csrf_token
        response = self.session.request(method, url, json=body, headers=headers, timeout=30)
        expected = (expected_status,) if isinstance(expected_status, int) else expected_status
        if response.status_code not in expected:
            raise RuntimeError(
                f"Unexpected status {response.status_code} for {url}. Body={response.text}"
            )
        return response

    def get_json(
        self,
        url: str,
        *,
        expected_status: int | tuple[int, ...] = 200,
    ) -> requests.Response:
        response = self.session.get(url, timeout=30)
        expected = (expected_status,) if isinstance(expected_status, int) else expected_status
        if response.status_code not in expected:
            raise RuntimeError(
                f"Unexpected status {response.status_code} for {url}. Body={response.text}"
            )
        return response


def run_cmd(args: list[str], env: dict[str, str] | None = None) -> None:
    print(f"$ {' '.join(args)}")
    result = subprocess.run(args, cwd=PROJECT_ROOT, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"command failed: {' '.join(args)}")


def wait_api() -> None:
    health_url = f"{API_BASE}/health"
    for _ in range(60):
        try:
            response = requests.get(health_url, timeout=2)
            if response.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise RuntimeError("API did not become healthy in time")


def login(
    client: CookieApiClient,
    username: str,
    password: str,
    *,
    totp_code: str | None = None,
    challenge_id: str | None = None,
) -> dict:
    body: dict[str, str] = {"username": username, "password": password}
    if totp_code:
        body["totp_code"] = totp_code
    if challenge_id:
        body["challenge_id"] = challenge_id
    response = client.request_json(
        "POST",
        f"{AUTH_PREFIX}/login",
        body,
        expected_status=(200, 401, 429),
        attach_csrf=False,
    )
    if response.status_code != 200:
        raise RuntimeError(f"Login failed for {username}: {response.status_code} {response.text}")
    return response.json()


def assert_logged_cookie(client: CookieApiClient) -> None:
    if not client.session.cookies.get(AUTH_COOKIE_NAME):
        raise RuntimeError("Expected auth cookie after login")
    if not client.session.cookies.get(CSRF_COOKIE_NAME):
        raise RuntimeError("Expected csrf cookie after login")


def main() -> int:
    compose_env = os.environ.copy()
    compose_env.setdefault("AUTH_BOOTSTRAP_ADMIN_USERNAME", BOOTSTRAP_ADMIN_USERNAME)
    compose_env.setdefault("AUTH_BOOTSTRAP_ADMIN_PASSWORD", BOOTSTRAP_ADMIN_PASSWORD)
    compose_env.setdefault("AUTH_JWT_SECRET", "change-me-in-production")
    compose_env.setdefault("AUTH_RECOVERY_PEPPER", "change-recovery-pepper")
    compose_env.setdefault("AUTH_SECRET_PROTECTION_KEY", "change-secret-protection-key")

    keep_stack = os.environ.get("KEEP_AUTH_RETEST_STACK", "false").lower() == "true"

    try:
        run_cmd(["docker", "compose", "down", "-v", "--remove-orphans"], env=compose_env)
        run_cmd(["docker", "compose", "up", "-d", "--build", "db", "redis", "api"], env=compose_env)
        wait_api()

        admin = CookieApiClient()

        # 1) login admin bootstrap (without MFA)
        admin_login = login(admin, BOOTSTRAP_ADMIN_USERNAME, BOOTSTRAP_ADMIN_PASSWORD)
        if "mfa_required" in admin_login:
            raise RuntimeError("Expected bootstrap admin login to authenticate directly")
        if "access_token" in admin_login:
            raise RuntimeError("/auth/login returned access_token, expected cookie-only contract")
        assert_logged_cookie(admin)

        me_admin = admin.get_json(f"{AUTH_PREFIX}/me", expected_status=200).json()
        if me_admin.get("username") != BOOTSTRAP_ADMIN_USERNAME:
            raise RuntimeError("/auth/me did not resolve admin user from cookie")

        # 2) create director user
        admin.request_json(
            "POST",
            f"{AUTH_PREFIX}/users",
            {
                "username": DIRECTOR_USERNAME,
                "password": DIRECTOR_PASSWORD,
                "role": "DIRECTOR",
            },
            expected_status=201,
        )

        # 3) setup+confirm MFA for admin
        setup_resp = admin.request_json("POST", f"{AUTH_PREFIX}/mfa/setup", {}, expected_status=200).json()
        otpauth_url = setup_resp["otpauth_url"]
        totp = pyotp.parse_uri(otpauth_url)
        confirm_resp = admin.request_json(
            "POST",
            f"{AUTH_PREFIX}/mfa/confirm",
            {"totp_code": totp.now()},
            expected_status=200,
        ).json()
        recovery_codes = confirm_resp.get("recovery_codes", [])
        if not recovery_codes:
            raise RuntimeError("Expected recovery codes after MFA confirmation")

        # 4) challenge flow login after MFA enabled
        admin = CookieApiClient()
        challenge_login = login(admin, BOOTSTRAP_ADMIN_USERNAME, BOOTSTRAP_ADMIN_PASSWORD)
        if not challenge_login.get("mfa_required"):
            raise RuntimeError("Expected MFA challenge response")
        challenge_id = challenge_login.get("challenge_id")
        if not challenge_id:
            raise RuntimeError("Missing challenge_id in MFA challenge response")

        admin_login_after_mfa = login(
            admin,
            BOOTSTRAP_ADMIN_USERNAME,
            BOOTSTRAP_ADMIN_PASSWORD,
            totp_code=totp.now(),
            challenge_id=challenge_id,
        )
        if "mfa_required" in admin_login_after_mfa:
            raise RuntimeError("Expected MFA login completion")
        assert_logged_cookie(admin)

        # 5) reset password with TOTP
        admin.request_json(
            "POST",
            f"{AUTH_PREFIX}/password/reset",
            {
                "username": BOOTSTRAP_ADMIN_USERNAME,
                "totp_code": totp.now(),
                "new_password": ADMIN_NEW_PASSWORD,
            },
            expected_status=200,
        )

        # login with new password (still MFA enabled)
        admin = CookieApiClient()
        challenge_login = login(admin, BOOTSTRAP_ADMIN_USERNAME, ADMIN_NEW_PASSWORD)
        challenge_id = challenge_login.get("challenge_id")
        if not challenge_id:
            raise RuntimeError("Expected challenge_id after password reset")
        login(
            admin,
            BOOTSTRAP_ADMIN_USERNAME,
            ADMIN_NEW_PASSWORD,
            totp_code=totp.now(),
            challenge_id=challenge_id,
        )
        assert_logged_cookie(admin)

        # 6) CSRF validation + admin access csv-tabs + datasets ingest
        profile_body = {
            "source": "AD",
            "scope": "PRIVATE",
            "name": "auth-retest-profile",
            "payload": {
                "header_row": 0,
                "sic_column": "Hostname",
                "selected_columns": ["Hostname", "OS"],
            },
        }

        admin.request_json(
            "POST",
            f"{API_PREFIX}/csv-tabs/profiles",
            profile_body,
            attach_csrf=False,
            expected_status=403,
        )

        profile = admin.request_json(
            "POST",
            f"{API_PREFIX}/csv-tabs/profiles",
            profile_body,
            expected_status=201,
        ).json()
        profile_id = profile["id"]

        admin.request_json(
            "POST",
            f"{API_PREFIX}/datasets/machines/ingest",
            {
                "data_dir": "/workspace",
                "profile_ids": {"AD": profile_id},
            },
            attach_csrf=False,
            expected_status=403,
        )

        admin.request_json(
            "POST",
            f"{API_PREFIX}/datasets/machines/ingest",
            {
                "data_dir": "/workspace",
                "profile_ids": {"AD": profile_id},
            },
            expected_status=200,
        )

        # 7) director permissions
        director = CookieApiClient()
        director_login = login(director, DIRECTOR_USERNAME, DIRECTOR_PASSWORD)
        if "mfa_required" in director_login:
            raise RuntimeError("director user unexpectedly requires MFA")
        assert_logged_cookie(director)

        director.request_json(
            "PUT",
            f"{API_PREFIX}/csv-tabs/profiles/{profile_id}",
            {
                "payload": {
                    "header_row": 0,
                    "sic_column": "Hostname",
                    "selected_columns": ["Hostname"],
                },
            },
            expected_status=403,
        )

        director.request_json(
            "POST",
            f"{API_PREFIX}/datasets/machines/ingest",
            {"data_dir": "/workspace", "profile_ids": {}},
            expected_status=403,
        )

        director.get_json(
            f"{API_PREFIX}/machines/table?page=1&size=10",
            expected_status=200,
        )

        print("AUTH retests passed")
        return 0
    except Exception as exc:
        print(f"AUTH retests failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if not keep_stack:
            try:
                run_cmd(["docker", "compose", "down", "-v", "--remove-orphans"], env=compose_env)
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
