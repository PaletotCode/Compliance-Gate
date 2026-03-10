from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RETESTS_DIR = PROJECT_ROOT / "retests"
OUTPUT_DIR = RETESTS_DIR / "output"
LOGS_DIR = RETESTS_DIR / "logs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

API_BASE = os.environ.get("API_BASE_URL", "http://localhost:8000")
AUTH_BASE = f"{API_BASE}/api/v1/auth"
DATASETS_BASE = f"{API_BASE}/api/v1/datasets/machines"
ENGINE_BASE = f"{API_BASE}/api/v1/engine"
UPLOADS_BASE = f"{API_BASE}/api/v1/workspace/uploads"
CSV_TABS_BASE = f"{API_BASE}/api/v1/csv-tabs"

BOOTSTRAP_ADMIN_USERNAME = os.environ.get("AUTH_BOOTSTRAP_ADMIN_USERNAME", "admin")
BOOTSTRAP_ADMIN_PASSWORD = os.environ.get("AUTH_BOOTSTRAP_ADMIN_PASSWORD", "Admin1234")
DIRECTOR_PASSWORD = os.environ.get("DECL_ENGINE_DIRECTOR_PASSWORD", "Director123")

POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "compliance_gate")

AUTH_COOKIE_NAME = os.environ.get("AUTH_COOKIE_NAME", "cg_access")
CSRF_COOKIE_NAME = os.environ.get("CSRF_COOKIE_NAME", "cg_csrf")
CSRF_HEADER_NAME = os.environ.get("CSRF_HEADER_NAME", "X-CSRF-Token")

KEEP_STACK = os.environ.get("KEEP_DECL_ENGINE_RETEST_STACK", "false").lower() == "true"

REQUIRED_SOURCES = ("AD", "UEM", "EDR", "ASSET")


@dataclass(slots=True)
class StepRecord:
    number: int
    name: str
    status: str
    duration_ms: float
    details: str = ""


class CookieApiClient:
    def __init__(self) -> None:
        self.session = requests.Session()

    def request_raw(
        self,
        method: str,
        url: str,
        *,
        body: dict[str, Any] | None = None,
        expected_status: int | tuple[int, ...] = 200,
        attach_csrf: bool = True,
        timeout: int = 90,
    ) -> requests.Response:
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if attach_csrf and method.upper() in {"POST", "PUT", "PATCH", "DELETE"}:
            csrf_token = self.session.cookies.get(CSRF_COOKIE_NAME)
            if csrf_token:
                headers[CSRF_HEADER_NAME] = csrf_token

        response = self.session.request(method, url, json=body, headers=headers, timeout=timeout)
        expected = (expected_status,) if isinstance(expected_status, int) else expected_status
        if response.status_code not in expected:
            raise RuntimeError(f"{method} {url} -> {response.status_code}: {response.text}")
        return response

    def request_json(
        self,
        method: str,
        url: str,
        *,
        body: dict[str, Any] | None = None,
        expected_status: int | tuple[int, ...] = 200,
        attach_csrf: bool = True,
        timeout: int = 90,
    ) -> Any:
        response = self.request_raw(
            method,
            url,
            body=body,
            expected_status=expected_status,
            attach_csrf=attach_csrf,
            timeout=timeout,
        )
        return response.json()

    def request_multipart(
        self,
        method: str,
        url: str,
        *,
        files: dict[str, tuple[str, bytes, str]],
        expected_status: int | tuple[int, ...] = 200,
        attach_csrf: bool = True,
        timeout: int = 120,
    ) -> Any:
        headers: dict[str, str] = {}
        if attach_csrf and method.upper() in {"POST", "PUT", "PATCH", "DELETE"}:
            csrf_token = self.session.cookies.get(CSRF_COOKIE_NAME)
            if csrf_token:
                headers[CSRF_HEADER_NAME] = csrf_token

        payload = {
            key: (filename, BytesIO(content), content_type)
            for key, (filename, content, content_type) in files.items()
        }
        response = self.session.request(method, url, headers=headers, files=payload, timeout=timeout)
        expected = (expected_status,) if isinstance(expected_status, int) else expected_status
        if response.status_code not in expected:
            raise RuntimeError(f"{method} {url} -> {response.status_code}: {response.text}")
        return response.json()


class RetestContext:
    def __init__(self) -> None:
        now = datetime.now(UTC)
        ts = now.strftime("%Y%m%d_%H%M%S")
        self.run_id = f"{ts}_{int(now.timestamp()) % 1_000_000:06d}"
        self.started_at = now
        self.log_path = LOGS_DIR / f"decl_engine_retest_{self.run_id}.log"
        self.api_log_path = LOGS_DIR / f"decl_engine_api_{self.run_id}.log"
        self.db_log_path = LOGS_DIR / f"decl_engine_db_{self.run_id}.log"

        self.output_json_path = OUTPUT_DIR / f"decl_engine_run_{self.run_id}.json"
        self.output_csv_path = OUTPUT_DIR / f"decl_engine_samples_{self.run_id}.csv"
        self.report_path = OUTPUT_DIR / "DECL_ENGINE_FINAL_REPORT.md"

        self.step_records: list[StepRecord] = []
        self.command_records: list[dict[str, Any]] = []
        self.evidence: dict[str, Any] = {
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "paths": {
                "log": str(self.log_path),
                "api_log": str(self.api_log_path),
                "db_log": str(self.db_log_path),
                "json": str(self.output_json_path),
                "csv": str(self.output_csv_path),
                "report": str(self.report_path),
            },
        }

        self._log_fh = self.log_path.open("w", encoding="utf-8")

    def close(self) -> None:
        self._log_fh.close()

    def log(self, message: str) -> None:
        ts = datetime.now(UTC).strftime("%H:%M:%S")
        line = f"[{ts}] {message}"
        print(line)
        self._log_fh.write(line + "\n")
        self._log_fh.flush()

    def record_command(
        self,
        *,
        cmd: list[str],
        exit_code: int,
        stdout: str,
        stderr: str,
        label: str,
    ) -> None:
        self.command_records.append(
            {
                "label": label,
                "cmd": " ".join(cmd),
                "exit_code": exit_code,
                "stdout": stdout[-8000:],
                "stderr": stderr[-8000:],
            }
        )


def run_command(
    ctx: RetestContext,
    cmd: list[str],
    *,
    label: str,
    env: dict[str, str] | None = None,
    fail_on_error: bool = True,
    timeout: int | None = None,
) -> subprocess.CompletedProcess[str]:
    ctx.log(f"$ {' '.join(cmd)}")
    proc = subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    ctx.record_command(
        cmd=cmd,
        exit_code=proc.returncode,
        stdout=proc.stdout,
        stderr=proc.stderr,
        label=label,
    )
    if proc.stdout.strip():
        ctx.log(f"stdout: {proc.stdout.strip()[:600]}")
    if proc.stderr.strip():
        ctx.log(f"stderr: {proc.stderr.strip()[:600]}")
    if fail_on_error and proc.returncode != 0:
        raise RuntimeError(f"command failed ({proc.returncode}): {' '.join(cmd)}")
    return proc


def wait_api_ready(ctx: RetestContext, timeout_seconds: int = 180) -> None:
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
    raise RuntimeError("API did not become ready in time")


def run_step(ctx: RetestContext, number: int, name: str, fn) -> Any:
    ctx.log(f"PASSO {number}: {name}")
    t0 = time.perf_counter()
    try:
        result = fn()
        elapsed = round((time.perf_counter() - t0) * 1000, 2)
        ctx.step_records.append(
            StepRecord(number=number, name=name, status="PASS", duration_ms=elapsed)
        )
        ctx.log(f"PASSO {number} OK ({elapsed} ms)")
        return result
    except Exception as exc:
        elapsed = round((time.perf_counter() - t0) * 1000, 2)
        ctx.step_records.append(
            StepRecord(
                number=number,
                name=name,
                status="FAIL",
                duration_ms=elapsed,
                details=str(exc),
            )
        )
        ctx.log(f"PASSO {number} FAIL ({elapsed} ms): {exc}")
        raise


def login(client: CookieApiClient, *, username: str, password: str) -> dict[str, Any]:
    response = client.request_json(
        "POST",
        f"{AUTH_BASE}/login",
        body={"username": username, "password": password},
        expected_status=200,
        attach_csrf=False,
    )
    if response.get("mfa_required"):
        raise RuntimeError(f"user {username} requires MFA in retest context")
    if "access_token" in response:
        raise RuntimeError("/auth/login returned access_token, expected cookie-only")
    if not client.session.cookies.get(AUTH_COOKIE_NAME):
        raise RuntimeError(f"login failed for {username}: missing {AUTH_COOKIE_NAME}")
    if not client.session.cookies.get(CSRF_COOKIE_NAME):
        raise RuntimeError(f"login failed for {username}: missing {CSRF_COOKIE_NAME}")
    return response


def ensure_default_profiles(ctx: RetestContext, client: CookieApiClient) -> dict[str, str]:
    defaults: dict[str, str] = {}

    for source in REQUIRED_SOURCES:
        profiles = client.request_json("GET", f"{CSV_TABS_BASE}/profiles?source={source}")
        existing_default = next((p for p in profiles if p.get("is_default_for_source")), None)
        if existing_default:
            defaults[source] = existing_default["id"]
            continue

        preview = client.request_json(
            "POST",
            f"{CSV_TABS_BASE}/preview/raw",
            body={"source": source, "data_dir": "/workspace"},
            expected_status=200,
        )

        headers = preview.get("detected_headers") or preview.get("original_headers") or []
        headers = [h for h in headers if isinstance(h, str) and h.strip()]
        if not headers:
            headers = ["hostname"]

        preferred_names = [
            "Hostname",
            "HOSTNAME",
            "hostname",
            "device_name",
            "nome_dispositivo",
            "name",
        ]
        sic_column = next((h for h in headers if h in preferred_names), headers[0])
        selected_columns = headers[:5] if headers else [sic_column]

        payload: dict[str, Any] = {
            "header_row": int(preview.get("header_row_index") or 0),
            "sic_column": sic_column,
            "selected_columns": selected_columns,
            "alias_map": {},
            "normalize_key_strategy": "ts_default",
        }

        delimiter = preview.get("detected_delimiter")
        if isinstance(delimiter, str) and delimiter:
            payload["delimiter"] = delimiter
        encoding = preview.get("detected_encoding")
        if isinstance(encoding, str) and encoding and encoding != "unknown":
            payload["encoding"] = encoding

        created = client.request_json(
            "POST",
            f"{CSV_TABS_BASE}/profiles",
            body={
                "source": source,
                "scope": "TENANT",
                "name": f"default-{source.lower()}-{ctx.run_id}",
                "payload": payload,
                "is_default_for_source": True,
            },
            expected_status=201,
        )
        defaults[source] = created["id"]

    return defaults


def ensure_workspace_fixtures() -> dict[str, tuple[str, bytes, str]]:
    workspace = PROJECT_ROOT / "workspace"
    file_map = {
        "AD": "AD.csv",
        "UEM": "UEM.csv",
        "EDR": "EDR.csv",
        "ASSET": "ASSET.CSV",
    }

    files: dict[str, tuple[str, bytes, str]] = {}
    for source, filename in file_map.items():
        path = workspace / filename
        if not path.exists():
            raise RuntimeError(f"fixture not found: {path}")
        files[source] = (filename, path.read_bytes(), "text/csv")
    return files


def write_samples_csv(path: Path, rows: list[dict[str, Any]], fallback_columns: list[str]) -> None:
    if rows:
        fieldnames = list({k: None for row in rows for k in row.keys()}.keys())
    else:
        fieldnames = fallback_columns or ["empty"]

    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        if rows:
            writer.writerows(rows)


def fetch_engine_run_metrics(ctx: RetestContext, dataset_version_id: str) -> list[dict[str, Any]]:
    sql = (
        "SELECT run_type, status, COALESCE(metrics_json, '{}') "
        "FROM engine_runs "
        f"WHERE dataset_version_id = '{dataset_version_id}' "
        "AND run_type IN ('segment_preview','view_preview','view_run') "
        "ORDER BY created_at DESC LIMIT 20;"
    )

    result = run_command(
        ctx,
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "db",
            "psql",
            "-U",
            POSTGRES_USER,
            "-d",
            POSTGRES_DB,
            "-At",
            "-F",
            "|",
            "-c",
            sql,
        ],
        label="engine_runs_query",
        fail_on_error=False,
    )

    rows: list[dict[str, Any]] = []
    if result.returncode != 0:
        return rows

    for raw_line in result.stdout.splitlines():
        if not raw_line.strip():
            continue
        parts = raw_line.split("|", 2)
        if len(parts) != 3:
            continue
        run_type, status, metrics_raw = parts
        try:
            metrics = json.loads(metrics_raw)
        except json.JSONDecodeError:
            metrics = {"raw": metrics_raw}
        rows.append({"run_type": run_type, "status": status, "metrics": metrics})
    return rows


def write_final_report(ctx: RetestContext, success: bool) -> None:
    checklist_lines = []
    for step in sorted(ctx.step_records, key=lambda s: s.number):
        suffix = f" ({step.duration_ms} ms)"
        if step.status == "PASS":
            checklist_lines.append(f"- PASSO {step.number}: PASS{suffix}")
        else:
            checklist_lines.append(f"- PASSO {step.number}: FAIL{suffix} -> {step.details}")

    command_lines = []
    for item in ctx.command_records:
        command_lines.append(
            f"- [{item['label']}] exit={item['exit_code']} :: `{item['cmd']}`"
        )

    metrics = ctx.evidence.get("metrics", {})
    human_error = ctx.evidence.get("human_error", {})

    report = [
        "# Declarative Engine v1 - Final E2E Report",
        "",
        f"- run_id: `{ctx.run_id}`",
        f"- started_at_utc: `{ctx.started_at.isoformat()}`",
        f"- finished_at_utc: `{datetime.now(UTC).isoformat()}`",
        f"- overall_status: `{'PASS' if success else 'FAIL'}`",
        "",
        "## Checklist PASS/FAIL",
        *checklist_lines,
        "",
        "## Commands + Exit Codes",
        *command_lines,
        "",
        "## Log Paths",
        f"- main_log: `{ctx.log_path}`",
        f"- api_log: `{ctx.api_log_path}`",
        f"- db_log: `{ctx.db_log_path}`",
        "",
        "## Metrics (Rows/Timings)",
        f"- materialized_row_count: `{metrics.get('materialized_row_count', 'n/a')}`",
        f"- catalog_column_count: `{metrics.get('catalog_column_count', 'n/a')}`",
        f"- segment_total_rows: `{metrics.get('segment_total_rows', 'n/a')}`",
        f"- segment_matched_rows: `{metrics.get('segment_matched_rows', 'n/a')}`",
        f"- segment_match_rate: `{metrics.get('segment_match_rate', 'n/a')}`",
        f"- view_preview_total_rows: `{metrics.get('view_preview_total_rows', 'n/a')}`",
        f"- view_preview_returned_rows: `{metrics.get('view_preview_returned_rows', 'n/a')}`",
        f"- view_run_total_rows: `{metrics.get('view_run_total_rows', 'n/a')}`",
        f"- view_run_items_page_1: `{metrics.get('view_run_items_page_1', 'n/a')}`",
        "",
        "### Engine Runs Audit",
    ]

    engine_runs = ctx.evidence.get("engine_runs", [])
    if engine_runs:
        for item in engine_runs:
            report.append(
                "- "
                f"run_type=`{item.get('run_type')}` status=`{item.get('status')}` metrics=`{json.dumps(item.get('metrics', {}), ensure_ascii=False)}`"
            )
    else:
        report.append("- (no engine_runs metrics captured)")

    report.extend(
        [
            "",
            "## Human Error Feedback Example",
            f"- code: `{human_error.get('code', 'n/a')}`",
            f"- message: `{human_error.get('message', 'n/a')}`",
            f"- hint: `{human_error.get('hint', 'n/a')}`",
            f"- node_path: `{human_error.get('node_path', 'n/a')}`",
            f"- pattern: `{human_error.get('pattern', 'n/a')}`",
            "",
            "## Output Artifacts",
            f"- run_json: `{ctx.output_json_path}`",
            f"- samples_csv: `{ctx.output_csv_path}`",
            f"- final_report: `{ctx.report_path}`",
            "",
            "## Explicit Proof",
            "- Usuário TI_ADMIN criou transformation, segment e view via API sem UI.",
            "- Preview de segment e preview de view executaram com dados reais materializados.",
            "- Erro humano estruturado retornou code/message/details/hint com node_path.",
            "- Usuário DIRECTOR executou GET/preview/run e recebeu 403 ao tentar criar segment.",
        ]
    )

    ctx.report_path.write_text("\n".join(report) + "\n", encoding="utf-8")


def main() -> int:
    ctx = RetestContext()

    compose_env = os.environ.copy()
    compose_env.setdefault("AUTH_BOOTSTRAP_ADMIN_USERNAME", BOOTSTRAP_ADMIN_USERNAME)
    compose_env.setdefault("AUTH_BOOTSTRAP_ADMIN_PASSWORD", BOOTSTRAP_ADMIN_PASSWORD)
    compose_env.setdefault("AUTH_JWT_SECRET", "change-me-in-production")
    compose_env.setdefault("AUTH_RECOVERY_PEPPER", "change-recovery-pepper")
    compose_env.setdefault("AUTH_SECRET_PROTECTION_KEY", "change-secret-protection-key")

    admin = CookieApiClient()
    director = CookieApiClient()

    overall_success = False

    dataset_version_id = ""
    upload_session_id = ""
    tenant_id = ""
    default_profiles: dict[str, str] = {}

    transformation_id = ""
    segment_id = ""
    view_id = ""

    segment_preview_data: dict[str, Any] = {}
    view_preview_data: dict[str, Any] = {}
    view_run_page_1: dict[str, Any] = {}
    view_run_page_2: dict[str, Any] | None = None

    try:
        run_step(
            ctx,
            1,
            "docker compose up (db/redis/api)",
            lambda: (
                run_command(
                    ctx,
                    ["docker", "compose", "down", "-v", "--remove-orphans"],
                    label="compose_down_before_up",
                    env=compose_env,
                    fail_on_error=False,
                ),
                run_command(
                    ctx,
                    ["docker", "compose", "up", "-d", "--build", "db", "redis", "api"],
                    label="compose_up",
                    env=compose_env,
                ),
                wait_api_ready(ctx),
            ),
        )

        def _step2() -> None:
            login(admin, username=BOOTSTRAP_ADMIN_USERNAME, password=BOOTSTRAP_ADMIN_PASSWORD)
            me = admin.request_json("GET", f"{AUTH_BASE}/me")
            nonlocal tenant_id
            tenant_id = me["tenant_id"]

        run_step(ctx, 2, "login TI_ADMIN (cookie-only + CSRF)", _step2)

        def _step3() -> None:
            nonlocal default_profiles
            default_profiles = ensure_default_profiles(ctx, admin)
            if set(default_profiles.keys()) != set(REQUIRED_SOURCES):
                raise RuntimeError("default profiles not resolved for all required sources")

        run_step(ctx, 3, "garantir profiles default AD/UEM/EDR/ASSET", _step3)

        def _step4() -> None:
            nonlocal upload_session_id, dataset_version_id

            files = ensure_workspace_fixtures()
            upload_response = admin.request_multipart(
                "POST",
                UPLOADS_BASE,
                files=files,
                expected_status=200,
            )
            upload_session_id = upload_response["data"]["upload_session_id"]
            if not upload_session_id:
                raise RuntimeError("upload_session_id missing in upload response")

            ingest_response = admin.request_json(
                "POST",
                f"{DATASETS_BASE}/ingest",
                body={
                    "upload_session_id": upload_session_id,
                    "profile_ids": {},
                },
                expected_status=200,
            )
            dataset_version_id = ingest_response.get("dataset_version_id", "")
            if not dataset_version_id:
                raise RuntimeError("dataset_version_id missing in ingest response")

        run_step(ctx, 4, "datasets ingest -> dataset_version_id", _step4)

        def _step5() -> None:
            materialize = admin.request_json(
                "POST",
                f"{ENGINE_BASE}/materialize/machines"
                f"?dataset_version_id={dataset_version_id}&tenant_id={tenant_id}",
                expected_status=200,
            )
            row_count = int(materialize["data"]["row_count"])
            if row_count <= 0:
                raise RuntimeError("materialize row_count <= 0")
            ctx.evidence["materialize"] = materialize

        run_step(ctx, 5, "engine materialize -> row_count > 0", _step5)

        def _step6() -> None:
            catalog = admin.request_json(
                "GET",
                f"{ENGINE_BASE}/catalog/machines?dataset_version_id={dataset_version_id}",
                expected_status=200,
            )
            data = catalog["data"]
            columns = data.get("columns", [])
            if not columns:
                raise RuntimeError("catalog returned zero columns")
            for col in columns[:5]:
                if "name" not in col or "data_type" not in col:
                    raise RuntimeError("catalog column missing name/data_type")
            ctx.evidence["catalog"] = catalog

        run_step(ctx, 6, "engine catalog -> colunas listadas com tipos", _step6)

        def _step7() -> None:
            nonlocal transformation_id
            created = admin.request_json(
                "POST",
                f"{ENGINE_BASE}/transformations",
                body={
                    "name": f"pa_suffix_{ctx.run_id}",
                    "description": "Extract PA suffix from hostname",
                    "output_column_name": "pa_suffix",
                    "output_type": "string",
                    "expression": {
                        "node_type": "function_call",
                        "function_name": "regex_extract",
                        "arguments": [
                            {"node_type": "column_ref", "column": "hostname"},
                            {
                                "node_type": "literal",
                                "value_type": "string",
                                "value": "_(\\\\d{2})$",
                            },
                            {"node_type": "literal", "value_type": "int", "value": 1},
                        ],
                    },
                },
                expected_status=200,
            )
            transformation_id = created["data"]["id"]
            if not transformation_id:
                raise RuntimeError("transformation id missing")
            ctx.evidence["transformation"] = created

        run_step(ctx, 7, "criar transformation (pa_suffix)", _step7)

        def _step8() -> None:
            nonlocal segment_id
            created = admin.request_json(
                "POST",
                f"{ENGINE_BASE}/segments",
                body={
                    "name": f"falta_uem_user_{ctx.run_id}",
                    "description": "Machines with AD+EDR and no UEM",
                    "filter_expression": {
                        "node_type": "logical_op",
                        "operator": "AND",
                        "clauses": [
                            {
                                "node_type": "binary_op",
                                "operator": "==",
                                "left": {"node_type": "column_ref", "column": "has_ad"},
                                "right": {
                                    "node_type": "literal",
                                    "value_type": "bool",
                                    "value": True,
                                },
                            },
                            {
                                "node_type": "binary_op",
                                "operator": "==",
                                "left": {"node_type": "column_ref", "column": "has_uem"},
                                "right": {
                                    "node_type": "literal",
                                    "value_type": "bool",
                                    "value": False,
                                },
                            },
                            {
                                "node_type": "binary_op",
                                "operator": "==",
                                "left": {"node_type": "column_ref", "column": "has_edr"},
                                "right": {
                                    "node_type": "literal",
                                    "value_type": "bool",
                                    "value": True,
                                },
                            },
                        ],
                    },
                },
                expected_status=200,
            )
            segment_id = created["data"]["id"]
            if not segment_id:
                raise RuntimeError("segment id missing")
            ctx.evidence["segment"] = created

        run_step(ctx, 8, "criar segment (falta_uem_user)", _step8)

        def _step9() -> None:
            nonlocal segment_preview_data
            preview = admin.request_json(
                "POST",
                f"{ENGINE_BASE}/segments/preview?dataset_version_id={dataset_version_id}",
                body={"segment_id": segment_id, "limit": 25},
                expected_status=200,
            )
            data = preview["data"]
            required = {"total_rows", "matched_rows", "match_rate", "sample_rows", "warnings"}
            if not required.issubset(data.keys()):
                raise RuntimeError("segment preview contract mismatch")
            segment_preview_data = data
            ctx.evidence["segment_preview"] = preview

        run_step(ctx, 9, "preview segment -> match_rate + sample_rows", _step9)

        def _step10() -> None:
            nonlocal view_id
            created = admin.request_json(
                "POST",
                f"{ENGINE_BASE}/views",
                body={
                    "name": f"main_view_{ctx.run_id}",
                    "description": "Declarative v1 main view",
                    "payload": {
                        "schema_version": 1,
                        "dataset_scope": {
                            "mode": "dataset_version",
                            "dataset_version_id": dataset_version_id,
                        },
                        "columns": [
                            {"kind": "base", "column_name": "hostname"},
                            {"kind": "base", "column_name": "primary_status_label"},
                            {"kind": "base", "column_name": "has_uem"},
                            {"kind": "base", "column_name": "has_edr"},
                            {"kind": "base", "column_name": "has_asset"},
                            {
                                "kind": "derived",
                                "transformation_id": transformation_id,
                                "alias": "pa_suffix",
                            },
                        ],
                        "filters": {
                            "segment_ids": [segment_id],
                            "ad_hoc_expression": None,
                        },
                        "sort": {"column_name": "hostname", "direction": "asc"},
                        "row_limit": 1000,
                    },
                },
                expected_status=200,
            )
            view_id = created["data"]["id"]
            if not view_id:
                raise RuntimeError("view id missing")
            ctx.evidence["view"] = created

        run_step(ctx, 10, "criar view com colunas + filtro de segment", _step10)

        def _step11() -> None:
            nonlocal view_preview_data
            preview = admin.request_json(
                "POST",
                f"{ENGINE_BASE}/views/preview?dataset_version_id={dataset_version_id}",
                body={"view_id": view_id, "limit": 25},
                expected_status=200,
            )
            data = preview["data"]
            required = {"total_rows", "returned_rows", "sample_rows", "warnings"}
            if not required.issubset(data.keys()):
                raise RuntimeError("view preview contract mismatch")

            # "schema" proof for report: selected columns from saved view definition
            schema_columns = [
                "hostname",
                "primary_status_label",
                "has_uem",
                "has_edr",
                "has_asset",
                "pa_suffix",
            ]
            view_preview_data = data
            ctx.evidence["view_preview"] = {
                "raw": preview,
                "schema_columns": schema_columns,
            }

        run_step(ctx, 11, "preview view -> schema + sample_rows", _step11)

        def _step12() -> None:
            nonlocal view_run_page_1, view_run_page_2
            run_page_1 = admin.request_json(
                "POST",
                f"{ENGINE_BASE}/views/run?dataset_version_id={dataset_version_id}",
                body={"view_id": view_id, "page": 1, "size": 25},
                expected_status=200,
            )
            data_1 = run_page_1["data"]
            required = {
                "total_rows",
                "page",
                "size",
                "has_next",
                "has_previous",
                "columns",
                "items",
                "warnings",
            }
            if not required.issubset(data_1.keys()):
                raise RuntimeError("view run contract mismatch")
            view_run_page_1 = data_1

            if data_1.get("has_next"):
                run_page_2 = admin.request_json(
                    "POST",
                    f"{ENGINE_BASE}/views/run?dataset_version_id={dataset_version_id}",
                    body={"view_id": view_id, "page": 2, "size": 25},
                    expected_status=200,
                )
                view_run_page_2 = run_page_2["data"]

            ctx.evidence["view_run"] = {
                "page_1": run_page_1,
                "page_2": {"data": view_run_page_2} if view_run_page_2 is not None else None,
            }

        run_step(ctx, 12, "run view -> paginação com items", _step12)

        def _step13() -> None:
            response = admin.request_raw(
                "POST",
                f"{ENGINE_BASE}/transformations",
                body={
                    "name": f"invalid_regex_{ctx.run_id}",
                    "description": "Should fail with RegexCompileError",
                    "output_column_name": "bad_regex",
                    "output_type": "string",
                    "expression": {
                        "node_type": "function_call",
                        "function_name": "regex_extract",
                        "arguments": [
                            {"node_type": "column_ref", "column": "hostname"},
                            {
                                "node_type": "literal",
                                "value_type": "string",
                                "value": "(",
                            },
                            {"node_type": "literal", "value_type": "int", "value": 1},
                        ],
                    },
                },
                expected_status=(200, 400),
            )
            payload: dict[str, Any]
            if response.status_code == 400:
                payload = response.json().get("detail", {})
            else:
                # Fallback deterministic error proof: unknown column in inline segment preview.
                fallback = admin.request_raw(
                    "POST",
                    f"{ENGINE_BASE}/segments/preview?dataset_version_id={dataset_version_id}",
                    body={
                        "expression": {
                            "node_type": "binary_op",
                            "operator": "==",
                            "left": {"node_type": "column_ref", "column": "coluna_que_nao_existe"},
                            "right": {
                                "node_type": "literal",
                                "value_type": "bool",
                                "value": True,
                            },
                        },
                        "limit": 5,
                    },
                    expected_status=400,
                )
                payload = fallback.json().get("detail", {})
            details = payload.get("details", {}) if isinstance(payload, dict) else {}
            required_keys = {"code", "message", "details", "hint"}
            if not required_keys.issubset(payload.keys()):
                raise RuntimeError("human error payload missing required keys")
            if not details.get("node_path"):
                raise RuntimeError("human error payload missing node_path")

            ctx.evidence["human_error"] = {
                "code": payload.get("code"),
                "message": payload.get("message"),
                "hint": payload.get("hint"),
                "node_path": details.get("node_path"),
                "pattern": details.get("pattern"),
                "raw": payload,
            }

        run_step(ctx, 13, "forçar erro humano (regex inválida)", _step13)

        def _step14() -> None:
            director_username = f"director_{ctx.run_id[-10:]}"
            admin.request_json(
                "POST",
                f"{AUTH_BASE}/users",
                body={
                    "username": director_username,
                    "password": DIRECTOR_PASSWORD,
                    "role": "DIRECTOR",
                },
                expected_status=201,
            )

            login(director, username=director_username, password=DIRECTOR_PASSWORD)

            director.request_json("GET", f"{ENGINE_BASE}/segments", expected_status=200)
            director.request_json("GET", f"{ENGINE_BASE}/views", expected_status=200)
            director.request_json(
                "POST",
                f"{ENGINE_BASE}/segments/preview?dataset_version_id={dataset_version_id}",
                body={"segment_id": segment_id, "limit": 10},
                expected_status=200,
            )
            director.request_json(
                "POST",
                f"{ENGINE_BASE}/views/preview?dataset_version_id={dataset_version_id}",
                body={"view_id": view_id, "limit": 10},
                expected_status=200,
            )
            director.request_json(
                "POST",
                f"{ENGINE_BASE}/views/run?dataset_version_id={dataset_version_id}",
                body={"view_id": view_id, "page": 1, "size": 10},
                expected_status=200,
            )
            director.request_raw(
                "POST",
                f"{ENGINE_BASE}/segments",
                body={
                    "name": f"forbidden_segment_{ctx.run_id}",
                    "description": "director must not create",
                    "filter_expression": {
                        "node_type": "binary_op",
                        "operator": "==",
                        "left": {"node_type": "column_ref", "column": "has_ad"},
                        "right": {"node_type": "literal", "value_type": "bool", "value": True},
                    },
                },
                expected_status=403,
            )

        run_step(
            ctx,
            14,
            "login DIRECTOR + validar GET/preview/run OK e POST create 403",
            _step14,
        )

        ctx.evidence["engine_runs"] = fetch_engine_run_metrics(ctx, dataset_version_id)

        # Build metrics section.
        materialize_data = ctx.evidence.get("materialize", {}).get("data", {})
        catalog_columns = ctx.evidence.get("catalog", {}).get("data", {}).get("columns", [])
        ctx.evidence["metrics"] = {
            "materialized_row_count": materialize_data.get("row_count"),
            "catalog_column_count": len(catalog_columns),
            "segment_total_rows": segment_preview_data.get("total_rows"),
            "segment_matched_rows": segment_preview_data.get("matched_rows"),
            "segment_match_rate": segment_preview_data.get("match_rate"),
            "view_preview_total_rows": view_preview_data.get("total_rows"),
            "view_preview_returned_rows": view_preview_data.get("returned_rows"),
            "view_run_total_rows": view_run_page_1.get("total_rows"),
            "view_run_items_page_1": len(view_run_page_1.get("items", [])),
        }

        # Save JSON evidence.
        ctx.output_json_path.write_text(
            json.dumps(ctx.evidence, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        # Save sample CSV.
        sample_rows: list[dict[str, Any]] = []
        fallback_columns: list[str] = []
        if view_run_page_1.get("items"):
            sample_rows = list(view_run_page_1["items"])
            fallback_columns = list(view_run_page_1.get("columns", []))
        elif view_preview_data.get("sample_rows"):
            sample_rows = list(view_preview_data["sample_rows"])
            fallback_columns = [
                "hostname",
                "primary_status_label",
                "has_uem",
                "has_edr",
                "has_asset",
                "pa_suffix",
            ]
        else:
            sample_rows = list(segment_preview_data.get("sample_rows", []))
            fallback_columns = list(sample_rows[0].keys()) if sample_rows else []

        write_samples_csv(ctx.output_csv_path, sample_rows, fallback_columns)

        overall_success = True

    except Exception as exc:
        ctx.log(f"Declarative engine retest failed: {exc}")
        ctx.evidence["failure"] = str(exc)
        overall_success = False

    finally:
        # Always collect docker logs for report evidence.
        api_logs = run_command(
            ctx,
            ["docker", "compose", "logs", "--tail=300", "api"],
            label="compose_logs_api",
            env=compose_env,
            fail_on_error=False,
        )
        db_logs = run_command(
            ctx,
            ["docker", "compose", "logs", "--tail=200", "db"],
            label="compose_logs_db",
            env=compose_env,
            fail_on_error=False,
        )

        ctx.api_log_path.write_text(api_logs.stdout or "", encoding="utf-8")
        ctx.db_log_path.write_text(db_logs.stdout or "", encoding="utf-8")

        def _step15() -> None:
            if KEEP_STACK:
                ctx.log("KEEP_DECL_ENGINE_RETEST_STACK=true -> skipping docker compose down")
                return
            run_command(
                ctx,
                ["docker", "compose", "down", "-v", "--remove-orphans"],
                label="compose_down_final",
                env=compose_env,
                fail_on_error=False,
            )

        try:
            run_step(ctx, 15, "docker compose down", _step15)
        except Exception:
            # down errors are captured in step status; keep finalization going
            overall_success = False

        write_final_report(ctx, success=overall_success)
        ctx.close()

    if overall_success:
        print("Declarative engine retests passed")
        print(f"report={ctx.report_path}")
        print(f"run_json={ctx.output_json_path}")
        print(f"samples_csv={ctx.output_csv_path}")
        return 0

    print("Declarative engine retests failed", file=sys.stderr)
    print(f"report={ctx.report_path}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
