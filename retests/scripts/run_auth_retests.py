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


def request_json(
    method: str,
    url: str,
    body: dict,
    *,
    token: str | None = None,
    expected_status: int | tuple[int, ...] = 200,
) -> requests.Response:
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    response = requests.request(method, url, json=body, headers=headers, timeout=30)
    expected = (expected_status,) if isinstance(expected_status, int) else expected_status
    if response.status_code not in expected:
        raise RuntimeError(
            f"Unexpected status {response.status_code} for {url}. Body={response.text}"
        )
    return response


def get_with_token(
    url: str,
    *,
    token: str,
    expected_status: int | tuple[int, ...] = 200,
) -> requests.Response:
    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    expected = (expected_status,) if isinstance(expected_status, int) else expected_status
    if response.status_code not in expected:
        raise RuntimeError(
            f"Unexpected status {response.status_code} for {url}. Body={response.text}"
        )
    return response


def login(username: str, password: str, *, totp_code: str | None = None, challenge_id: str | None = None) -> dict:
    body: dict[str, str] = {"username": username, "password": password}
    if totp_code:
        body["totp_code"] = totp_code
    if challenge_id:
        body["challenge_id"] = challenge_id
    response = request_json("POST", f"{AUTH_PREFIX}/login", body, expected_status=(200, 401, 429))
    if response.status_code != 200:
        raise RuntimeError(f"Login failed for {username}: {response.status_code} {response.text}")
    return response.json()


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

        # 1) login admin bootstrap (without MFA)
        admin_login = login(BOOTSTRAP_ADMIN_USERNAME, BOOTSTRAP_ADMIN_PASSWORD)
        if "access_token" not in admin_login:
            raise RuntimeError("Expected bootstrap admin login to return access token")
        admin_token = admin_login["access_token"]

        # 2) create director user
        request_json(
            "POST",
            f"{AUTH_PREFIX}/users",
            {
                "username": DIRECTOR_USERNAME,
                "password": DIRECTOR_PASSWORD,
                "role": "DIRECTOR",
            },
            token=admin_token,
            expected_status=201,
        )

        # 3) setup+confirm MFA for admin
        setup_resp = request_json("POST", f"{AUTH_PREFIX}/mfa/setup", {}, token=admin_token, expected_status=200).json()
        otpauth_url = setup_resp["otpauth_url"]
        totp = pyotp.parse_uri(otpauth_url)
        confirm_resp = request_json(
            "POST",
            f"{AUTH_PREFIX}/mfa/confirm",
            {"totp_code": totp.now()},
            token=admin_token,
            expected_status=200,
        ).json()
        recovery_codes = confirm_resp.get("recovery_codes", [])
        if not recovery_codes:
            raise RuntimeError("Expected recovery codes after MFA confirmation")

        # 4) challenge flow login after MFA enabled
        challenge_login = login(BOOTSTRAP_ADMIN_USERNAME, BOOTSTRAP_ADMIN_PASSWORD)
        if not challenge_login.get("mfa_required"):
            raise RuntimeError("Expected MFA challenge response")
        challenge_id = challenge_login.get("challenge_id")
        if not challenge_id:
            raise RuntimeError("Missing challenge_id in MFA challenge response")

        admin_login_after_mfa = login(
            BOOTSTRAP_ADMIN_USERNAME,
            BOOTSTRAP_ADMIN_PASSWORD,
            totp_code=totp.now(),
            challenge_id=challenge_id,
        )
        admin_token = admin_login_after_mfa["access_token"]

        # 5) reset password with TOTP
        request_json(
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
        challenge_login = login(BOOTSTRAP_ADMIN_USERNAME, ADMIN_NEW_PASSWORD)
        challenge_id = challenge_login.get("challenge_id")
        admin_token = login(
            BOOTSTRAP_ADMIN_USERNAME,
            ADMIN_NEW_PASSWORD,
            totp_code=totp.now(),
            challenge_id=challenge_id,
        )["access_token"]

        # 6) admin access csv-tabs + datasets ingest
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
        profile = request_json(
            "POST",
            f"{API_PREFIX}/csv-tabs/profiles",
            profile_body,
            token=admin_token,
            expected_status=201,
        ).json()
        profile_id = profile["id"]

        request_json(
            "POST",
            f"{API_PREFIX}/datasets/machines/ingest",
            {
                "data_dir": "/workspace",
                "profile_ids": {"AD": profile_id},
            },
            token=admin_token,
            expected_status=200,
        )

        # 7) director permissions
        director_token = login(DIRECTOR_USERNAME, DIRECTOR_PASSWORD)["access_token"]

        request_json(
            "PUT",
            f"{API_PREFIX}/csv-tabs/profiles/{profile_id}",
            {
                "payload": {
                    "header_row": 0,
                    "sic_column": "Hostname",
                    "selected_columns": ["Hostname"],
                },
            },
            token=director_token,
            expected_status=403,
        )

        request_json(
            "POST",
            f"{API_PREFIX}/datasets/machines/ingest",
            {"data_dir": "/workspace", "profile_ids": {}},
            token=director_token,
            expected_status=403,
        )

        get_with_token(
            f"{API_PREFIX}/machines/table?page=1&size=10",
            token=director_token,
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
