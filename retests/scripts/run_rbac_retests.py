from __future__ import annotations

import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
API_BASE = os.environ.get("API_BASE_URL", "http://localhost:8000")
AUTH_BASE = f"{API_BASE}/api/v1/auth"
MACHINES_BASE = f"{API_BASE}/api/v1/machines"
DATASETS_BASE = f"{API_BASE}/api/v1/datasets/machines"

BOOTSTRAP_ADMIN_USERNAME = os.environ.get("AUTH_BOOTSTRAP_ADMIN_USERNAME", "admin")
BOOTSTRAP_ADMIN_PASSWORD = os.environ.get("AUTH_BOOTSTRAP_ADMIN_PASSWORD", "Admin1234")
TENANT_B_USER = "tenant_b_director"
TENANT_B_PASSWORD = "Director123"


def run_cmd(
    args: list[str], *, env: dict[str, str] | None = None, fail: bool = True
) -> subprocess.CompletedProcess[str]:
    print(f"$ {' '.join(args)}")
    result = subprocess.run(args, cwd=PROJECT_ROOT, env=env, capture_output=True, text=True)
    if fail and result.returncode != 0:
        print(result.stdout)
        print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"command failed: {' '.join(args)}")
    return result


def wait_api(timeout_seconds: int = 120) -> None:
    url = f"{API_BASE}/health"
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            response = requests.get(url, timeout=3)
            if response.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise RuntimeError("API did not become healthy in time")


def request_json(
    method: str,
    url: str,
    *,
    body: dict | None = None,
    token: str | None = None,
    expected_status: int | tuple[int, ...] = 200,
) -> requests.Response:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    response = requests.request(method, url, json=body, headers=headers, timeout=30)
    expected = (expected_status,) if isinstance(expected_status, int) else expected_status
    if response.status_code not in expected:
        raise RuntimeError(
            f"Unexpected status {response.status_code} for {method} {url}. Body={response.text}"
        )
    return response


def login(username: str, password: str) -> str:
    response = request_json(
        "POST",
        f"{AUTH_BASE}/login",
        body={"username": username, "password": password},
        expected_status=200,
    ).json()
    token = response.get("access_token")
    if not token:
        raise RuntimeError(f"login failed for {username}: missing access_token")
    return token


def seed_tenant_b_user_in_db(user_id: str, tenant_b_id: str) -> None:
    tenant_slug = "tenant-b"
    sql = (
        "INSERT INTO tenants (id, slug, display_name, name, is_active, created_at) "
        f"VALUES ('{tenant_b_id}', '{tenant_slug}', 'Tenant B', 'Tenant B', true, now()) "
        "ON CONFLICT (slug) DO NOTHING; "
        f"UPDATE users SET tenant_id = '{tenant_b_id}' WHERE id = '{user_id}';"
    )
    run_cmd(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "db",
            "psql",
            "-U",
            os.environ.get("POSTGRES_USER", "postgres"),
            "-d",
            os.environ.get("POSTGRES_DB", "compliance_gate"),
            "-c",
            sql,
        ]
    )


def main() -> int:
    compose_env = os.environ.copy()
    compose_env.setdefault("AUTH_BOOTSTRAP_ADMIN_USERNAME", BOOTSTRAP_ADMIN_USERNAME)
    compose_env.setdefault("AUTH_BOOTSTRAP_ADMIN_PASSWORD", BOOTSTRAP_ADMIN_PASSWORD)
    compose_env.setdefault("AUTH_JWT_SECRET", "change-me-in-production")
    compose_env.setdefault("AUTH_RECOVERY_PEPPER", "change-recovery-pepper")
    compose_env.setdefault("AUTH_SECRET_PROTECTION_KEY", "change-secret-protection-key")

    keep_stack = os.environ.get("KEEP_RBAC_RETEST_STACK", "false").lower() == "true"

    try:
        run_cmd(
            ["docker", "compose", "down", "-v", "--remove-orphans"], env=compose_env, fail=False
        )
        run_cmd(["docker", "compose", "up", "-d", "--build", "db", "redis", "api"], env=compose_env)
        wait_api()

        # 1) /machines/table without token -> 401
        response = requests.get(f"{MACHINES_BASE}/table?page=1&size=10", timeout=30)
        if response.status_code != 401:
            raise RuntimeError(
                f"expected 401 without token, got {response.status_code}: {response.text}"
            )

        # 2) tenant A admin setup + ingest
        admin_token = login(BOOTSTRAP_ADMIN_USERNAME, BOOTSTRAP_ADMIN_PASSWORD)
        ingest = request_json(
            "POST",
            f"{DATASETS_BASE}/ingest",
            body={"data_dir": "/workspace", "profile_ids": {}},
            token=admin_token,
            expected_status=200,
        ).json()
        dataset_version_id = ingest.get("dataset_version_id")
        if not dataset_version_id:
            raise RuntimeError("ingest did not return dataset_version_id")

        # 3) create user via API, then move to tenant B directly in DB
        created_user = request_json(
            "POST",
            f"{AUTH_BASE}/users",
            body={"username": TENANT_B_USER, "password": TENANT_B_PASSWORD, "role": "DIRECTOR"},
            token=admin_token,
            expected_status=201,
        ).json()
        user_id = created_user["id"]
        tenant_b_id = str(uuid.uuid4())
        seed_tenant_b_user_in_db(user_id, tenant_b_id)

        # 4) tenant B token attempting tenant A dataset_version -> 404 (or empty list)
        tenant_b_token = login(TENANT_B_USER, TENANT_B_PASSWORD)
        cross = requests.get(
            f"{MACHINES_BASE}/table?page=1&size=10&dataset_version_id={dataset_version_id}",
            headers={"Authorization": f"Bearer {tenant_b_token}"},
            timeout=30,
        )
        if cross.status_code == 200:
            payload = cross.json().get("data", {})
            items = payload.get("items", []) if isinstance(payload, dict) else []
            if items:
                raise RuntimeError("cross-tenant request returned non-empty items")
        elif cross.status_code != 404:
            raise RuntimeError(
                f"expected 404 or empty list for cross-tenant access, got {cross.status_code}: {cross.text}"
            )

        # 5) auth retests must stay green
        auth_retests = run_cmd(
            [sys.executable, "retests/scripts/run_auth_retests.py"],
            env=compose_env,
            fail=False,
        )
        if auth_retests.returncode != 0:
            print(auth_retests.stdout)
            print(auth_retests.stderr, file=sys.stderr)
            raise RuntimeError("run_auth_retests.py failed")

        print("RBAC retests passed")
        return 0
    except Exception as exc:
        print(f"RBAC retests failed: {exc}", file=sys.stderr)
        run_cmd(["docker", "compose", "logs", "--tail=200", "api"], env=compose_env, fail=False)
        return 1
    finally:
        if not keep_stack:
            run_cmd(
                ["docker", "compose", "down", "-v", "--remove-orphans"], env=compose_env, fail=False
            )


if __name__ == "__main__":
    raise SystemExit(main())
