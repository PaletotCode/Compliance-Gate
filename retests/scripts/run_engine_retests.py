from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "retests" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

API_BASE = os.environ.get("API_BASE_URL", "http://localhost:8000")
AUTH_BASE = f"{API_BASE}/api/v1/auth"
DATASETS_BASE = f"{API_BASE}/api/v1/datasets/machines"
ENGINE_BASE = f"{API_BASE}/api/v1/engine"

BOOTSTRAP_ADMIN_USERNAME = os.environ.get("AUTH_BOOTSTRAP_ADMIN_USERNAME", "admin")
BOOTSTRAP_ADMIN_PASSWORD = os.environ.get("AUTH_BOOTSTRAP_ADMIN_PASSWORD", "Admin1234")
KEEP_STACK = os.environ.get("KEEP_ENGINE_RETEST_STACK", "false").lower() == "true"


def run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


def run_command(cmd: list[str], fail_on_error: bool = True) -> subprocess.CompletedProcess[str]:
    print(f"$ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
    if fail_on_error and result.returncode != 0:
        print(result.stdout)
        print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"command failed: {' '.join(cmd)}")
    return result


def wait_api_ready(timeout_seconds: int = 120) -> None:
    deadline = time.time() + timeout_seconds
    health_url = f"{API_BASE}/health"
    while time.time() < deadline:
        try:
            response = requests.get(health_url, timeout=3)
            if response.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise RuntimeError("API failed to become ready")


def request_json(
    method: str,
    url: str,
    *,
    token: str | None = None,
    body: dict | None = None,
    expected_status: int | tuple[int, ...] = 200,
) -> dict:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    response = requests.request(method, url, headers=headers, json=body, timeout=60)
    expected = (expected_status,) if isinstance(expected_status, int) else expected_status
    if response.status_code not in expected:
        raise RuntimeError(f"{method} {url} -> {response.status_code}: {response.text}")
    return response.json()


def translate_workspace_path(container_path: str) -> Path:
    prefix = "/workspace/"
    if container_path.startswith(prefix):
        relative = container_path[len(prefix):]
        return PROJECT_ROOT / "workspace" / relative
    return Path(container_path)


def main() -> int:
    current_run = run_id()

    try:
        run_command(["docker", "compose", "down", "-v", "--remove-orphans"], fail_on_error=False)
        run_command(["docker", "compose", "up", "-d", "--build", "db", "redis", "api"])
        wait_api_ready()

        # 1) login bootstrap admin
        login_payload = {
            "username": BOOTSTRAP_ADMIN_USERNAME,
            "password": BOOTSTRAP_ADMIN_PASSWORD,
        }
        login_response = request_json("POST", f"{AUTH_BASE}/login", body=login_payload)
        token = login_response.get("access_token")
        if not token:
            raise RuntimeError("bootstrap admin login did not return access token")

        me_response = request_json("GET", f"{AUTH_BASE}/me", token=token)
        tenant_id = me_response["tenant_id"]

        # 2) ingest dataset for engine consumption
        ingest_response = request_json(
            "POST",
            f"{DATASETS_BASE}/ingest",
            token=token,
            body={"data_dir": "/workspace", "profile_ids": {}},
            expected_status=200,
        )
        dataset_version_id = ingest_response.get("dataset_version_id")
        if not dataset_version_id:
            raise RuntimeError("datasets ingest did not return dataset_version_id")

        # 3) materialize machines parquet
        materialize_url = (
            f"{ENGINE_BASE}/materialize/machines?dataset_version_id={dataset_version_id}&tenant_id={tenant_id}"
        )
        materialize_response = request_json("POST", materialize_url, token=token)
        materialize_data = materialize_response["data"]

        if materialize_data["row_count"] <= 0:
            raise RuntimeError("materialization produced zero rows")

        parquet_path = translate_workspace_path(materialize_data["path"])
        if not parquet_path.exists():
            raise RuntimeError(f"parquet file not found at {parquet_path}")

        materialize_output = OUTPUT_DIR / f"engine_materialize_{current_run}.json"
        materialize_output.write_text(
            json.dumps(materialize_response, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        # 4) run required report template
        report_url = f"{ENGINE_BASE}/reports/run?dataset_version_id={dataset_version_id}&tenant_id={tenant_id}"
        report_response = request_json(
            "POST",
            report_url,
            token=token,
            body={"template_name": "machines_status_summary"},
        )
        report_data = report_response["data"]
        rows = report_data["data"]
        if not rows:
            raise RuntimeError("report returned no rows")

        # 5) consistency validation
        status_total = sum(int(row["count"]) for row in rows if row.get("type") == "status")
        if status_total != int(materialize_data["row_count"]):
            raise RuntimeError(
                f"status count mismatch: report={status_total} materialized={materialize_data['row_count']}"
            )

        report_output = OUTPUT_DIR / f"engine_report_{current_run}.json"
        report_output.write_text(
            json.dumps(report_response, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        report_csv = OUTPUT_DIR / f"engine_report_table_{current_run}.csv"
        with report_csv.open("w", encoding="utf-8", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

        print("Engine retests passed")
        print(f"materialize_output={materialize_output}")
        print(f"report_output={report_output}")
        print(f"report_table={report_csv}")
        return 0

    except Exception as exc:
        print(f"Engine retests failed: {exc}", file=sys.stderr)
        run_command(["docker", "compose", "logs", "--tail=200", "api"], fail_on_error=False)
        run_command(["docker", "compose", "logs", "--tail=120", "db"], fail_on_error=False)
        return 1

    finally:
        if not KEEP_STACK:
            run_command(["docker", "compose", "down", "-v", "--remove-orphans"], fail_on_error=False)


if __name__ == "__main__":
    raise SystemExit(main())
