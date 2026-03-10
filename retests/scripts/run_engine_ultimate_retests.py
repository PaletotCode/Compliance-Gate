from __future__ import annotations

import csv
import hashlib
import json
import os
import random
import socket
import statistics
import subprocess
import sys
import time
import traceback
from collections import Counter
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

RETESTS_DIR = PROJECT_ROOT / "retests"
OUTPUT_DIR = RETESTS_DIR / "output"
LOGS_DIR = RETESTS_DIR / "logs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

PYTEST_BIN = PROJECT_ROOT / ".venv" / "bin" / "pytest"
PYTHON_BIN = PROJECT_ROOT / ".venv" / "bin" / "python3.14"
if not PYTHON_BIN.exists():
    PYTHON_BIN = PROJECT_ROOT / ".venv" / "bin" / "python"
NPM_BIN = "npm"
ALEMBIC_BIN = PROJECT_ROOT / ".venv" / "bin" / "alembic"

PARITY_THRESHOLD = float(os.environ.get("ENGINE_ULTIMATE_PARITY_THRESHOLD", "99.9"))
PERF_CLASSIFICATION_P99_MS = float(
    os.environ.get("ENGINE_ULTIMATE_PERF_CLASSIFICATION_P99_MS", "12000")
)
PERF_PREVIEW_P99_MS = float(os.environ.get("ENGINE_ULTIMATE_PERF_PREVIEW_P99_MS", "8000"))
PERF_RUN_P99_MS = float(os.environ.get("ENGINE_ULTIMATE_PERF_RUN_P99_MS", "14000"))


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _run_id() -> str:
    ts = _utc_now().strftime("%Y%m%d_%H%M%S")
    short = hashlib.sha1(f"{ts}-{time.time_ns()}".encode("utf-8")).hexdigest()[:8]
    return f"{ts}_{short}"


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    index = (len(ordered) - 1) * (p / 100.0)
    low = int(index)
    high = min(low + 1, len(ordered) - 1)
    frac = index - low
    return round((ordered[low] * (1.0 - frac)) + (ordered[high] * frac), 4)


def _safe_json(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, dict):
        return {str(k): _safe_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_safe_json(v) for v in value]
    return str(value)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@dataclass(slots=True)
class CommandRecord:
    check_id: str
    command: str
    cwd: str
    exit_code: int
    duration_ms: float
    log_path: str
    timeout_seconds: int | None


@dataclass(slots=True)
class CheckRecord:
    block_id: str
    block_name: str
    check_id: str
    status: str
    message: str
    severity: str
    metric_name: str | None = None
    metric_value: str | None = None
    threshold: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class BlockResult:
    block_id: str
    block_name: str
    started_at: str
    ended_at: str
    duration_ms: float
    status: str
    checks_total: int
    checks_passed: int
    checks_failed: int
    log_path: str
    details: dict[str, Any] = field(default_factory=dict)
    commands: list[CommandRecord] = field(default_factory=list)
    failures: list[dict[str, Any]] = field(default_factory=list)


class BlockContext:
    def __init__(
        self,
        *,
        runner: "UltimateRetestRunner",
        block_id: str,
        block_name: str,
        log_path: Path,
    ) -> None:
        self.runner = runner
        self.block_id = block_id
        self.block_name = block_name
        self.log_path = log_path
        self.started_at = _utc_now()
        self.commands: list[CommandRecord] = []
        self.checks: list[CheckRecord] = []
        self.failures: list[dict[str, Any]] = []
        self.details: dict[str, Any] = {}

    def log(self, message: str) -> None:
        ts = _utc_now().strftime("%H:%M:%S")
        line = f"[{ts}] [{self.block_id}] {message}"
        print(line)
        with self.log_path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")


class UltimateRetestRunner:
    def __init__(self) -> None:
        self.run_id = _run_id()
        self.started_at = _utc_now()
        self.block_logs_dir = LOGS_DIR / f"engine_ultimate_{self.run_id}"
        self.block_logs_dir.mkdir(parents=True, exist_ok=True)

        self.report_path = OUTPUT_DIR / "ENGINE_ULTIMATE_CERTIFICATION_REPORT.md"
        self.run_json_path = OUTPUT_DIR / f"engine_ultimate_run_{self.run_id}.json"
        self.matrix_csv_path = OUTPUT_DIR / f"engine_ultimate_matrix_{self.run_id}.csv"
        self.failures_json_path = OUTPUT_DIR / f"engine_ultimate_failures_{self.run_id}.json"

        self.block_results: list[BlockResult] = []
        self.matrix: list[CheckRecord] = []
        self.failures: list[dict[str, Any]] = []
        self.global_metrics: dict[str, Any] = {
            "parity": {},
            "performance": {},
            "rollback": {},
            "error_contract": {},
        }

    def _record_check(
        self,
        ctx: BlockContext,
        *,
        check_id: str,
        status: str,
        message: str,
        severity: str = "normal",
        metric_name: str | None = None,
        metric_value: Any = None,
        threshold: Any = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        check = CheckRecord(
            block_id=ctx.block_id,
            block_name=ctx.block_name,
            check_id=check_id,
            status=status,
            message=message,
            severity=severity,
            metric_name=metric_name,
            metric_value=str(metric_value) if metric_value is not None else None,
            threshold=str(threshold) if threshold is not None else None,
            details=details or {},
        )
        ctx.checks.append(check)
        self.matrix.append(check)
        if status != "PASS":
            failure = {
                "block_id": ctx.block_id,
                "block_name": ctx.block_name,
                "check_id": check_id,
                "message": message,
                "severity": severity,
                "details": _safe_json(details or {}),
            }
            ctx.failures.append(failure)
            self.failures.append(failure)
            ctx.log(f"FAIL {check_id}: {message}")
        else:
            ctx.log(f"PASS {check_id}: {message}")

    def _exec(
        self,
        ctx: BlockContext,
        *,
        check_id: str,
        cmd: list[str],
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        timeout_seconds: int | None = None,
    ) -> subprocess.CompletedProcess[str]:
        resolved_cwd = cwd or PROJECT_ROOT
        command_text = " ".join(cmd)
        ctx.log(f"$ {command_text}")
        started = time.perf_counter()
        completed = subprocess.run(
            cmd,
            cwd=resolved_cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
        elapsed_ms = round((time.perf_counter() - started) * 1000.0, 2)
        cmd_log = self.block_logs_dir / f"{ctx.block_id}_{check_id}.log"
        cmd_log.write_text(
            (
                f"$ {command_text}\n"
                f"cwd: {resolved_cwd}\n"
                f"exit_code: {completed.returncode}\n"
                f"elapsed_ms: {elapsed_ms}\n\n"
                f"[stdout]\n{completed.stdout}\n\n[stderr]\n{completed.stderr}\n"
            ),
            encoding="utf-8",
        )
        record = CommandRecord(
            check_id=check_id,
            command=command_text,
            cwd=str(resolved_cwd),
            exit_code=completed.returncode,
            duration_ms=elapsed_ms,
            log_path=str(cmd_log),
            timeout_seconds=timeout_seconds,
        )
        ctx.commands.append(record)
        return completed

    def _run_block(self, block_id: str, block_name: str, fn) -> None:
        log_path = self.block_logs_dir / f"{block_id}.log"
        log_path.write_text("", encoding="utf-8")
        ctx = BlockContext(runner=self, block_id=block_id, block_name=block_name, log_path=log_path)
        ctx.log(f"START {block_name}")
        try:
            fn(ctx)
        except Exception as exc:  # pragma: no cover - orchestration guard
            self._record_check(
                ctx,
                check_id="unhandled_exception",
                status="FAIL",
                message=f"Unhandled exception: {type(exc).__name__}: {exc}",
                severity="critical",
                details={"traceback": traceback.format_exc()},
            )
        finally:
            ended_at = _utc_now()
            duration_ms = round((ended_at - ctx.started_at).total_seconds() * 1000.0, 2)
            checks_total = len(ctx.checks)
            checks_failed = len([c for c in ctx.checks if c.status != "PASS"])
            checks_passed = checks_total - checks_failed
            status = "PASS" if checks_failed == 0 else "FAIL"
            ctx.log(f"END {block_name} -> {status}")
            self.block_results.append(
                BlockResult(
                    block_id=ctx.block_id,
                    block_name=ctx.block_name,
                    started_at=ctx.started_at.isoformat(),
                    ended_at=ended_at.isoformat(),
                    duration_ms=duration_ms,
                    status=status,
                    checks_total=checks_total,
                    checks_passed=checks_passed,
                    checks_failed=checks_failed,
                    log_path=str(ctx.log_path),
                    details=_safe_json(ctx.details),
                    commands=ctx.commands,
                    failures=ctx.failures,
                )
            )

    @staticmethod
    def _build_synthetic_records(seed: int, size: int, profile: str) -> list[dict[str, Any]]:
        rnd = random.Random(seed)
        now_ms = int(time.time() * 1000)
        records: list[dict[str, Any]] = []
        legacy_os = [
            "Windows 7",
            "Windows 8",
            "Windows XP",
            "Windows Server 2008",
            "Windows Server 2012",
        ]
        modern_os = ["Windows 10", "Windows 11", "Ubuntu 22.04", "macOS 14"]

        rule_cases: list[dict[str, Any]] = [
            {
                "hostname": "PA_00_00_01",
                "pa_code": "01",
                "is_virtual_gap": True,
                "is_available_in_asset": False,
                "has_ad": False,
                "has_uem": False,
                "has_edr": False,
                "has_asset": False,
            },
            {
                "hostname": "PA_00_00_02",
                "pa_code": "02",
                "is_virtual_gap": False,
                "is_available_in_asset": True,
                "has_ad": False,
                "has_uem": False,
                "has_edr": False,
                "has_asset": True,
            },
            {
                "hostname": "PA_00_00_03",
                "pa_code": "03",
                "has_ad": False,
                "has_uem": True,
                "has_edr": False,
                "has_asset": True,
            },
            {
                "hostname": "PA_00_00_04",
                "pa_code": "04",
                "has_ad": False,
                "has_uem": False,
                "has_edr": False,
                "has_asset": False,
            },
            {
                "hostname": "PA_00_00_05",
                "pa_code": "05",
                "has_ad": True,
                "has_uem": False,
                "has_edr": False,
                "has_asset": True,
            },
            {
                "hostname": "PA_00_00_06",
                "pa_code": "06",
                "has_ad": True,
                "has_uem": False,
                "has_edr": True,
                "has_asset": True,
            },
            {
                "hostname": "PA_00_00_07",
                "pa_code": "07",
                "has_ad": True,
                "has_uem": True,
                "has_edr": False,
                "has_asset": True,
            },
            {
                "hostname": "PA_00_00_08",
                "pa_code": "08",
                "has_ad": True,
                "has_uem": True,
                "has_edr": False,
                "has_asset": False,
            },
            {
                "hostname": "PA_00_00_09",
                "pa_code": "09",
                "has_ad": True,
                "has_uem": True,
                "has_edr": True,
                "has_asset": True,
                "uem_serial": "SER-1",
                "edr_serial": "SER-2",
            },
            {
                "hostname": "PA_00_00_10",
                "pa_code": "10",
                "has_ad": True,
                "has_uem": True,
                "has_edr": True,
                "has_asset": True,
                "serial_is_cloned": True,
            },
            {
                "hostname": "PA_00_00_11",
                "pa_code": "11",
                "has_ad": True,
                "has_uem": True,
                "has_edr": True,
                "has_asset": True,
                "last_seen_date_ms": now_ms - (80 * 24 * 60 * 60 * 1000),
            },
            {
                "hostname": "PA_00_00_12",
                "pa_code": "12",
                "has_ad": True,
                "has_uem": True,
                "has_edr": True,
                "has_asset": True,
                "ad_os": "Windows 7",
            },
            {
                "hostname": "PA_00_00_13",
                "pa_code": "13",
                "has_ad": True,
                "has_uem": True,
                "has_edr": True,
                "has_asset": True,
                "main_user": "corp\\operator_13",
                "uem_extra_user_logado": "operator_12",
            },
        ]

        for idx in range(size):
            if idx < len(rule_cases):
                base = dict(rule_cases[idx])
            else:
                suffix = f"{idx % 99:02d}"
                hostname = f"PA_{profile}_{idx // 100:03d}_{suffix}"
                has_ad = rnd.choice([True, False])
                has_uem = rnd.choice([True, False])
                has_edr = rnd.choice([True, False])
                has_asset = rnd.choice([True, False])
                uem_serial = None
                edr_serial = None
                if rnd.random() < 0.35:
                    uem_serial = f"U-{rnd.randint(10000, 99999)}"
                if rnd.random() < 0.35:
                    edr_serial = f"E-{rnd.randint(10000, 99999)}"
                if uem_serial and edr_serial and rnd.random() < 0.25:
                    edr_serial = uem_serial
                days_old = rnd.randint(0, 120)
                base = {
                    "hostname": hostname,
                    "pa_code": suffix,
                    "has_ad": has_ad,
                    "has_uem": has_uem,
                    "has_edr": has_edr,
                    "has_asset": has_asset,
                    "ad_os": rnd.choice(legacy_os + modern_os),
                    "uem_serial": uem_serial,
                    "edr_serial": edr_serial,
                    "main_user": f"corp\\user_{suffix}",
                    "uem_extra_user_logado": (
                        f"user_{suffix}"
                        if rnd.random() > 0.2
                        else f"user_{rnd.randint(0, 99):02d}"
                    ),
                    "serial_is_cloned": rnd.random() < 0.02,
                    "is_virtual_gap": False,
                    "is_available_in_asset": False,
                    "last_seen_date_ms": now_ms - (days_old * 24 * 60 * 60 * 1000),
                }
                if rnd.random() < 0.01:
                    base["is_virtual_gap"] = True
                if rnd.random() < 0.01:
                    base["is_available_in_asset"] = True

            record = {
                "hostname": str(base.get("hostname", f"HOST-{idx}")),
                "pa_code": str(base.get("pa_code", f"{idx%99:02d}")),
                "has_ad": bool(base.get("has_ad", False)),
                "has_uem": bool(base.get("has_uem", False)),
                "has_edr": bool(base.get("has_edr", False)),
                "has_asset": bool(base.get("has_asset", False)),
                "ad_os": base.get("ad_os"),
                "uem_serial": base.get("uem_serial"),
                "edr_serial": base.get("edr_serial"),
                "chassis": None,
                "edr_os": None,
                "us_ad": None,
                "us_uem": None,
                "us_edr": None,
                "main_user": base.get("main_user"),
                "uem_extra_user_logado": base.get("uem_extra_user_logado"),
                "status_check_win11": None,
                "last_seen_date_ms": int(base.get("last_seen_date_ms", now_ms)),
                "serial_is_cloned": bool(base.get("serial_is_cloned", False)),
                "is_virtual_gap": bool(base.get("is_virtual_gap", False)),
                "is_available_in_asset": bool(base.get("is_available_in_asset", False)),
                "raw_sources": {},
            }
            records.append(record)
        return records

    def block_01_unit_contract_migration(self, ctx: BlockContext) -> None:
        checks = [
            (
                "pytest_engine_suite",
                [str(PYTEST_BIN), "-q", "src/compliance_gate/tests/engine"],
                900,
            ),
            (
                "pytest_contract_suite",
                [
                    str(PYTEST_BIN),
                    "-q",
                    "src/compliance_gate/tests/authentication",
                    "src/compliance_gate/tests/domains/test_main_view_backend_contracts.py",
                    "src/compliance_gate/tests/domains/test_machines_rbac.py",
                ],
                900,
            ),
            (
                "pytest_backward_compat_legacy",
                [
                    str(PYTEST_BIN),
                    "-q",
                    "src/compliance_gate/tests/engine/test_rulesets_runtime.py::test_legacy_mode_uses_legacy_classifier",
                ],
                300,
            ),
        ]
        for check_id, cmd, timeout in checks:
            result = self._exec(ctx, check_id=check_id, cmd=cmd, timeout_seconds=timeout)
            self._record_check(
                ctx,
                check_id=check_id,
                status="PASS" if result.returncode == 0 else "FAIL",
                message=f"exit_code={result.returncode}",
                details={"command": " ".join(cmd)},
            )

        migration_container = f"cg-ultimate-mig-{self.run_id[-8:]}"
        pg_port = _free_port()
        db_url = f"postgresql+psycopg2://postgres:postgres@localhost:{pg_port}/cg_migration_check"
        env = os.environ.copy()
        env["DATABASE_URL"] = db_url

        try:
            self._exec(
                ctx,
                check_id="mig_container_cleanup_pre",
                cmd=["docker", "rm", "-f", migration_container],
                timeout_seconds=60,
            )
            start = self._exec(
                ctx,
                check_id="mig_container_start",
                cmd=[
                    "docker",
                    "run",
                    "-d",
                    "--name",
                    migration_container,
                    "-e",
                    "POSTGRES_PASSWORD=postgres",
                    "-e",
                    "POSTGRES_USER=postgres",
                    "-e",
                    "POSTGRES_DB=cg_migration_check",
                    "-p",
                    f"{pg_port}:5432",
                    "postgres:16-alpine",
                ],
                timeout_seconds=240,
            )
            self._record_check(
                ctx,
                check_id="migration_container_started",
                status="PASS" if start.returncode == 0 else "FAIL",
                message=f"container={migration_container}",
                details={"port": pg_port},
            )

            ready = False
            for _ in range(70):
                probe = self._exec(
                    ctx,
                    check_id="mig_pg_ready_probe",
                    cmd=[
                        "docker",
                        "exec",
                        migration_container,
                        "pg_isready",
                        "-U",
                        "postgres",
                        "-d",
                        "cg_migration_check",
                    ],
                    timeout_seconds=20,
                )
                if probe.returncode == 0:
                    ready = True
                    break
                time.sleep(1)
            self._record_check(
                ctx,
                check_id="migration_db_ready",
                status="PASS" if ready else "FAIL",
                message="postgres readiness probe",
            )
            if ready:
                for check_id, command in [
                    ("alembic_upgrade_head_1", [str(ALEMBIC_BIN), "upgrade", "head"]),
                    (
                        "alembic_downgrade_target",
                        [str(ALEMBIC_BIN), "downgrade", "e1f8c9b2d0aa"],
                    ),
                    ("alembic_upgrade_head_2", [str(ALEMBIC_BIN), "upgrade", "head"]),
                ]:
                    result = self._exec(
                        ctx,
                        check_id=check_id,
                        cmd=command,
                        env=env,
                        timeout_seconds=300,
                    )
                    self._record_check(
                        ctx,
                        check_id=check_id,
                        status="PASS" if result.returncode == 0 else "FAIL",
                        message=f"exit_code={result.returncode}",
                        details={"database_url": db_url},
                    )
        finally:
            self._exec(
                ctx,
                check_id="mig_container_cleanup_post",
                cmd=["docker", "rm", "-f", migration_container],
                timeout_seconds=90,
            )

    def _compile_baseline(self):
        from compliance_gate.Engine.rulesets import (
            build_legacy_baseline_ruleset_payload,
            compile_ruleset_from_payload,
        )

        payload = build_legacy_baseline_ruleset_payload(stale_days=45)
        compiled = compile_ruleset_from_payload(
            payload,
            ruleset_name="ultimate-baseline",
            version=1,
        )
        return payload, compiled

    @staticmethod
    def _outputs_digest(batch) -> str:
        rows = []
        for item in batch.outputs:
            rows.append(
                {
                    "primary_status": item.primary_status,
                    "primary_status_label": item.primary_status_label,
                    "flags": list(item.flags),
                    "matched_rule_keys": list(item.matched_rule_keys),
                }
            )
        raw = json.dumps(rows, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def block_02_runtime_determinism(self, ctx: BlockContext) -> None:
        from compliance_gate.Engine.rulesets import ClassificationRuntimeMode, classify_records

        _, compiled = self._compile_baseline()
        records = self._build_synthetic_records(seed=101, size=2500, profile="DET")

        batch_1 = classify_records(
            records,
            mode=ClassificationRuntimeMode.DECLARATIVE,
            compiled_ruleset=compiled,
            context={"stale_days": 45},
        )
        batch_2 = classify_records(
            records,
            mode=ClassificationRuntimeMode.DECLARATIVE,
            compiled_ruleset=compiled,
            context={"stale_days": 45},
        )
        batch_3 = classify_records(
            records,
            mode=ClassificationRuntimeMode.DECLARATIVE,
            compiled_ruleset=compiled,
            context={"stale_days": 45},
        )

        d1 = self._outputs_digest(batch_1)
        d2 = self._outputs_digest(batch_2)
        d3 = self._outputs_digest(batch_3)
        deterministic = d1 == d2 == d3 and batch_1.metrics.rule_hits == batch_2.metrics.rule_hits
        self._record_check(
            ctx,
            check_id="same_input_same_output",
            status="PASS" if deterministic else "FAIL",
            message=f"digest1={d1[:10]} digest2={d2[:10]} digest3={d3[:10]}",
            details={
                "rows": len(records),
                "rule_hits_1": dict(batch_1.metrics.rule_hits),
                "rule_hits_2": dict(batch_2.metrics.rule_hits),
            },
        )

        shadow_1 = classify_records(
            records,
            mode=ClassificationRuntimeMode.SHADOW,
            compiled_ruleset=compiled,
            context={"stale_days": 45},
        )
        shadow_2 = classify_records(
            records,
            mode=ClassificationRuntimeMode.SHADOW,
            compiled_ruleset=compiled,
            context={"stale_days": 45},
        )
        same_divergence = len(shadow_1.divergences) == len(shadow_2.divergences)
        self._record_check(
            ctx,
            check_id="shadow_idempotent",
            status="PASS" if same_divergence else "FAIL",
            message=f"divergences_run1={len(shadow_1.divergences)} divergences_run2={len(shadow_2.divergences)}",
        )

        ctx.details["determinism"] = {
            "digest_declarative": d1,
            "shadow_divergences": len(shadow_1.divergences),
        }

    def block_03_legacy_parity(self, ctx: BlockContext) -> None:
        from compliance_gate.Engine.rulesets import ClassificationRuntimeMode, classify_records

        _, compiled = self._compile_baseline()
        datasets = [
            ("tenant_a", "dataset_small", self._build_synthetic_records(111, 1200, "A")),
            ("tenant_b", "dataset_medium", self._build_synthetic_records(222, 2800, "B")),
            ("tenant_c", "dataset_large", self._build_synthetic_records(333, 4500, "C")),
        ]

        parity_rows: list[dict[str, Any]] = []
        min_parity = 100.0
        by_kind: Counter[str] = Counter()
        by_severity: Counter[str] = Counter()
        by_rule: Counter[str] = Counter()

        for tenant_id, dataset_id, rows in datasets:
            batch = classify_records(
                rows,
                mode=ClassificationRuntimeMode.SHADOW,
                compiled_ruleset=compiled,
                context={"stale_days": 45},
            )
            total = len(rows)
            divergences = len(batch.divergences)
            parity = round(((total - divergences) / total) * 100.0, 6) if total > 0 else 0.0
            min_parity = min(min_parity, parity)
            for diff in batch.divergences:
                by_kind[str(diff.divergence_kind)] += 1
                by_severity[str(diff.severity)] += 1
                for rule_key in diff.rule_keys:
                    by_rule[str(rule_key)] += 1
            parity_rows.append(
                {
                    "tenant_id": tenant_id,
                    "dataset_id": dataset_id,
                    "rows": total,
                    "divergences": divergences,
                    "parity_percent": parity,
                }
            )
            self._record_check(
                ctx,
                check_id=f"parity_{tenant_id}_{dataset_id}",
                status="PASS" if parity >= PARITY_THRESHOLD else "FAIL",
                message=f"parity={parity:.4f}% threshold={PARITY_THRESHOLD:.4f}%",
                metric_name="parity_percent",
                metric_value=parity,
                threshold=PARITY_THRESHOLD,
                details={"rows": total, "divergences": divergences},
            )

        ctx.details["parity_rows"] = parity_rows
        ctx.details["divergences_by_kind"] = dict(by_kind)
        ctx.details["divergences_by_severity"] = dict(by_severity)
        ctx.details["divergences_by_rule"] = dict(by_rule)
        ctx.details["minimum_parity_percent"] = min_parity

        self.global_metrics["parity"] = {
            "threshold_percent": PARITY_THRESHOLD,
            "minimum_percent": min_parity,
            "datasets": parity_rows,
            "by_kind": dict(by_kind),
            "by_severity": dict(by_severity),
            "by_rule": dict(by_rule),
        }

    @staticmethod
    def _payload_with_single_rule(condition: dict[str, Any], output: dict[str, Any], *, kind: str = "primary"):
        from compliance_gate.Engine.rulesets import RuleSetPayloadV2

        return RuleSetPayloadV2.model_validate(
            {
                "schema_version": 2,
                "blocks": [
                    {
                        "kind": kind,
                        "entries": [
                            {
                                "rule_key": "r1",
                                "priority": 1,
                                "condition": condition,
                                "output": output,
                            }
                        ],
                    }
                ],
            }
        )

    def block_04_error_robustness(self, ctx: BlockContext) -> None:
        from compliance_gate.Engine.errors import GuardrailViolation
        from compliance_gate.Engine.expressions import ExpressionValidationOptions
        from compliance_gate.Engine.rulesets import (
            ClassificationRuntimeMode,
            compile_ruleset_from_payload,
            dry_run_ruleset,
            validate_ruleset_payload,
        )
        from compliance_gate.Engine.rulesets import runtime as runtime_module

        columns = runtime_module.machine_record_column_types()

        def check_issue_contract(issue: dict[str, Any]) -> bool:
            required = {"code", "message", "details", "hint", "node_path"}
            return required.issubset(issue.keys())

        unknown_column_payload = self._payload_with_single_rule(
            {
                "node_type": "binary_op",
                "operator": "==",
                "left": {"node_type": "column_ref", "column": "hostnme"},
                "right": {"node_type": "literal", "value_type": "string", "value": "HOST"},
            },
            {"primary_status": "COMPLIANT"},
        )
        unknown_column = validate_ruleset_payload(unknown_column_payload, column_types=columns)
        issue_1 = unknown_column["issues"][0]
        pass_unknown = (
            issue_1.get("code") == "UnknownColumn"
            and check_issue_contract(issue_1)
            and isinstance(issue_1.get("details", {}).get("suggestions"), list)
            and len(issue_1.get("details", {}).get("suggestions", [])) <= 3
        )
        self._record_check(
            ctx,
            check_id="error_unknown_column",
            status="PASS" if pass_unknown else "FAIL",
            message=f"code={issue_1.get('code')}",
            details=issue_1,
        )

        regex_payload = self._payload_with_single_rule(
            {
                "node_type": "function_call",
                "function_name": "regex_match",
                "arguments": [
                    {"node_type": "column_ref", "column": "hostname"},
                    {"node_type": "literal", "value_type": "string", "value": "("},
                ],
            },
            {"primary_status": "COMPLIANT"},
        )
        regex_result = validate_ruleset_payload(regex_payload, column_types=columns)
        issue_2 = regex_result["issues"][0]
        self._record_check(
            ctx,
            check_id="error_regex_compile",
            status="PASS" if issue_2.get("code") == "RegexCompileError" and check_issue_contract(issue_2) else "FAIL",
            message=f"code={issue_2.get('code')}",
            details=issue_2,
        )

        output_conflict_payload = self._payload_with_single_rule(
            {"node_type": "literal", "value_type": "bool", "value": True},
            {"primary_status": "COMPLIANT", "status": "ROGUE"},
        )
        conflict_result = validate_ruleset_payload(output_conflict_payload, column_types=columns)
        issue_3 = conflict_result["issues"][0]
        self._record_check(
            ctx,
            check_id="error_rule_output_conflict",
            status="PASS" if issue_3.get("code") == "RuleOutputConflict" and check_issue_contract(issue_3) else "FAIL",
            message=f"code={issue_3.get('code')}",
            details=issue_3,
        )

        from compliance_gate.Engine.rulesets import RuleSetPayloadV2

        unreachable_payload = RuleSetPayloadV2.model_validate(
            {
                "schema_version": 2,
                "blocks": [
                    {
                        "kind": "primary",
                        "entries": [
                            {
                                "rule_key": "always",
                                "priority": 1,
                                "condition": {"node_type": "literal", "value_type": "bool", "value": True},
                                "output": {"primary_status": "COMPLIANT"},
                            },
                            {
                                "rule_key": "never",
                                "priority": 2,
                                "condition": {"node_type": "literal", "value_type": "bool", "value": True},
                                "output": {"primary_status": "ROGUE"},
                            },
                        ],
                    }
                ],
            }
        )
        unreachable_result = validate_ruleset_payload(unreachable_payload, column_types=columns)
        warning_codes = [w.get("code") for w in unreachable_result.get("warnings", [])]
        self._record_check(
            ctx,
            check_id="warning_unreachable_rule",
            status="PASS" if "UnreachableRuleWarning" in warning_codes else "FAIL",
            message=f"warnings={warning_codes}",
        )

        invalid_syntax_payload = self._payload_with_single_rule(
            {
                "node_type": "function_call",
                "function_name": "regex_match",
                "arguments": [{"node_type": "column_ref", "column": "hostname"}],
            },
            {"primary_status": "COMPLIANT"},
        )
        invalid_syntax_result = validate_ruleset_payload(invalid_syntax_payload, column_types=columns)
        issue_4 = invalid_syntax_result["issues"][0]
        self._record_check(
            ctx,
            check_id="error_invalid_expression_syntax",
            status="PASS" if issue_4.get("code") == "InvalidExpressionSyntax" and check_issue_contract(issue_4) else "FAIL",
            message=f"code={issue_4.get('code')}",
            details=issue_4,
        )

        clauses = []
        for idx in range(40):
            clauses.append(
                {
                    "node_type": "binary_op",
                    "operator": "==",
                    "left": {"node_type": "column_ref", "column": "has_ad"},
                    "right": {"node_type": "literal", "value_type": "bool", "value": idx % 2 == 0},
                }
            )
        complexity_payload = self._payload_with_single_rule(
            {"node_type": "logical_op", "operator": "AND", "clauses": clauses},
            {"primary_status": "COMPLIANT"},
        )
        complexity_result = validate_ruleset_payload(
            complexity_payload,
            column_types=columns,
            options=ExpressionValidationOptions(max_nodes=12, max_depth=6),
        )
        issue_5 = complexity_result["issues"][0]
        self._record_check(
            ctx,
            check_id="error_excessive_complexity",
            status="PASS" if issue_5.get("code") == "ExcessiveComplexity" else "FAIL",
            message=f"code={issue_5.get('code')}",
            details=issue_5,
        )

        baseline_payload, compiled = self._compile_baseline()
        divergent_payload = self._payload_with_single_rule(
            {"node_type": "literal", "value_type": "bool", "value": True},
            {"primary_status": "ROGUE", "primary_status_label": "Rogue"},
        )
        divergent_compiled = compile_ruleset_from_payload(divergent_payload, ruleset_name="divergent", version=1)
        rows = self._build_synthetic_records(seed=77, size=35, profile="ERR")
        dry_run = dry_run_ruleset(
            divergent_compiled,
            rows=rows,
            mode=ClassificationRuntimeMode.SHADOW,
            explain_sample_limit=2,
        )
        warning_codes = [w.get("code") for w in dry_run.get("warnings", [])]
        self._record_check(
            ctx,
            check_id="warning_shadow_divergence",
            status="PASS" if "ShadowDivergenceWarning" in warning_codes else "FAIL",
            message=f"warnings={warning_codes}",
        )

        fuzz_failures = 0
        for idx in range(30):
            if idx % 3 == 0:
                condition = {
                    "node_type": "binary_op",
                    "operator": "==",
                    "left": {"node_type": "column_ref", "column": f"unknown_col_{idx}"},
                    "right": {"node_type": "literal", "value_type": "bool", "value": True},
                }
            elif idx % 3 == 1:
                condition = {
                    "node_type": "function_call",
                    "function_name": "regex_match",
                    "arguments": [
                        {"node_type": "column_ref", "column": "hostname"},
                        {"node_type": "literal", "value_type": "string", "value": "("},
                    ],
                }
            else:
                condition = {
                    "node_type": "function_call",
                    "function_name": "regex_match",
                    "arguments": [{"node_type": "column_ref", "column": "hostname"}],
                }
            payload = self._payload_with_single_rule(condition, {"primary_status": "COMPLIANT"})
            try:
                res = validate_ruleset_payload(payload, column_types=columns)
                if not res.get("issues"):
                    fuzz_failures += 1
                    continue
                first = res["issues"][0]
                if not check_issue_contract(first):
                    fuzz_failures += 1
            except Exception:
                fuzz_failures += 1
        self._record_check(
            ctx,
            check_id="fuzz_invalid_expressions",
            status="PASS" if fuzz_failures == 0 else "FAIL",
            message=f"fuzz_cases=30 failures={fuzz_failures}",
            details={"fuzz_failures": fuzz_failures},
        )

        payload_guardrail = GuardrailViolation(
            "guardrail",
            details={"reason": "test", "node_path": "root"},
            hint="hint",
        ).to_dict()
        contract_ok = {"code", "message", "details", "hint", "node_path"}.issubset(
            payload_guardrail.keys()
        )
        self._record_check(
            ctx,
            check_id="error_payload_contract_shape",
            status="PASS" if contract_ok else "FAIL",
            message=f"keys={sorted(payload_guardrail.keys())}",
            details=payload_guardrail,
        )

        ctx.details["error_examples"] = {
            "unknown_column": issue_1,
            "regex_compile": issue_2,
            "rule_output_conflict": issue_3,
            "invalid_syntax": issue_4,
            "complexity": issue_5,
        }
        self.global_metrics["error_contract"] = {
            "sample_unknown_column": issue_1,
            "sample_regex_compile": issue_2,
        }

    def block_05_guardrails_security(self, ctx: BlockContext) -> None:
        from compliance_gate.Engine.expressions import ExpressionValidationOptions
        from compliance_gate.Engine.rulesets import (
            ClassificationRuntimeMode,
            classify_records,
            compile_ruleset_from_payload,
            validate_ruleset_payload,
        )
        from compliance_gate.Engine.rulesets import runtime as runtime_module

        _, compiled = self._compile_baseline()

        deep_node: dict[str, Any] = {"node_type": "column_ref", "column": "has_ad"}
        for _ in range(25):
            deep_node = {"node_type": "unary_op", "operator": "NOT", "operand": deep_node}
        deep_payload = self._payload_with_single_rule(
            deep_node,
            {"primary_status": "COMPLIANT"},
        )
        deep_result = validate_ruleset_payload(
            deep_payload,
            column_types=runtime_module.machine_record_column_types(),
            options=ExpressionValidationOptions(max_nodes=80, max_depth=8),
        )
        deep_code = deep_result["issues"][0]["code"] if deep_result["issues"] else None
        self._record_check(
            ctx,
            check_id="guardrail_depth_overflow",
            status="PASS" if deep_code == "ExcessiveComplexity" else "FAIL",
            message=f"code={deep_code}",
        )

        timeout_guard = False
        try:
            runtime_module._check_deadline(deadline=time.perf_counter() - 0.0001, row_index=3)
        except Exception as exc:
            timeout_guard = type(exc).__name__ == "GuardrailViolation"
        self._record_check(
            ctx,
            check_id="guardrail_timeout_hard",
            status="PASS" if timeout_guard else "FAIL",
            message="deadline guard raises structured error",
        )

        injection_payload = self._payload_with_single_rule(
            {
                "node_type": "binary_op",
                "operator": "==",
                "left": {"node_type": "column_ref", "column": "hostname"},
                "right": {
                    "node_type": "literal",
                    "value_type": "string",
                    "value": "'; DROP TABLE engine_rule_sets; --",
                },
            },
            {"primary_status": "ROGUE"},
        )
        compiled_injection = compile_ruleset_from_payload(
            injection_payload,
            ruleset_name="inj",
            version=1,
        )
        rows = self._build_synthetic_records(seed=909, size=120, profile="INJ")
        classification = classify_records(
            rows,
            mode=ClassificationRuntimeMode.DECLARATIVE,
            compiled_ruleset=compiled_injection,
            context={"stale_days": 45},
        )
        sane = classification.metrics.rows_classified == len(rows)
        self._record_check(
            ctx,
            check_id="security_payload_injection_no_effect",
            status="PASS" if sane else "FAIL",
            message=f"rows_classified={classification.metrics.rows_classified}",
        )

        endpoint_guardrail = self._exec(
            ctx,
            check_id="guardrail_pytest_pagination_preview_sort",
            cmd=[
                str(PYTEST_BIN),
                "-q",
                "src/compliance_gate/tests/engine/test_declarative_runtime.py",
                "src/compliance_gate/tests/engine/test_report_validation.py",
                "src/compliance_gate/tests/domains/test_machines_endpoints.py",
            ],
            timeout_seconds=600,
        )
        self._record_check(
            ctx,
            check_id="guardrail_pytest_pagination_preview_sort",
            status="PASS" if endpoint_guardrail.returncode == 0 else "FAIL",
            message=f"exit_code={endpoint_guardrail.returncode}",
        )

    def block_06_performance_scale(self, ctx: BlockContext) -> None:
        from compliance_gate.Engine.rulesets import ClassificationRuntimeMode, dry_run_ruleset, explain_sample
        from compliance_gate.Engine.rulesets import classify_records

        _, compiled = self._compile_baseline()
        classification_samples: list[float] = []
        preview_samples: list[float] = []
        run_samples: list[float] = []

        workloads = [(1200, 5), (4500, 4), (9000, 3)]
        for size, loops in workloads:
            rows = self._build_synthetic_records(seed=size, size=size, profile=f"P{size}")
            for iteration in range(loops):
                start = time.perf_counter()
                batch = classify_records(
                    rows,
                    mode=ClassificationRuntimeMode.DECLARATIVE,
                    compiled_ruleset=compiled,
                    context={"stale_days": 45},
                )
                classification_ms = round((time.perf_counter() - start) * 1000.0, 4)
                classification_samples.append(classification_ms)
                if batch.metrics.rows_classified != len(rows):
                    self._record_check(
                        ctx,
                        check_id=f"classification_row_integrity_{size}_{iteration}",
                        status="FAIL",
                        message=f"rows_classified={batch.metrics.rows_classified} expected={len(rows)}",
                    )
                else:
                    self._record_check(
                        ctx,
                        check_id=f"classification_row_integrity_{size}_{iteration}",
                        status="PASS",
                        message=f"rows={len(rows)}",
                    )

                preview_rows = rows[: min(250, len(rows))]
                start = time.perf_counter()
                explain_sample(compiled, rows=preview_rows, limit=25)
                preview_ms = round((time.perf_counter() - start) * 1000.0, 4)
                preview_samples.append(preview_ms)

                run_rows = rows[: min(1200, len(rows))]
                start = time.perf_counter()
                dry_run_ruleset(
                    compiled,
                    rows=run_rows,
                    mode=ClassificationRuntimeMode.DECLARATIVE,
                    explain_sample_limit=5,
                )
                run_ms = round((time.perf_counter() - start) * 1000.0, 4)
                run_samples.append(run_ms)

        perf = {
            "classification": {
                "p50_ms": _percentile(classification_samples, 50),
                "p95_ms": _percentile(classification_samples, 95),
                "p99_ms": _percentile(classification_samples, 99),
                "samples": classification_samples,
            },
            "preview": {
                "p50_ms": _percentile(preview_samples, 50),
                "p95_ms": _percentile(preview_samples, 95),
                "p99_ms": _percentile(preview_samples, 99),
                "samples": preview_samples,
            },
            "run": {
                "p50_ms": _percentile(run_samples, 50),
                "p95_ms": _percentile(run_samples, 95),
                "p99_ms": _percentile(run_samples, 99),
                "samples": run_samples,
            },
        }
        self.global_metrics["performance"] = perf
        ctx.details["performance"] = perf

        self._record_check(
            ctx,
            check_id="perf_classification_p99",
            status="PASS"
            if perf["classification"]["p99_ms"] <= PERF_CLASSIFICATION_P99_MS
            else "FAIL",
            message=f"p99={perf['classification']['p99_ms']}ms",
            metric_name="classification_p99_ms",
            metric_value=perf["classification"]["p99_ms"],
            threshold=PERF_CLASSIFICATION_P99_MS,
        )
        self._record_check(
            ctx,
            check_id="perf_preview_p99",
            status="PASS" if perf["preview"]["p99_ms"] <= PERF_PREVIEW_P99_MS else "FAIL",
            message=f"p99={perf['preview']['p99_ms']}ms",
            metric_name="preview_p99_ms",
            metric_value=perf["preview"]["p99_ms"],
            threshold=PERF_PREVIEW_P99_MS,
        )
        self._record_check(
            ctx,
            check_id="perf_run_p99",
            status="PASS" if perf["run"]["p99_ms"] <= PERF_RUN_P99_MS else "FAIL",
            message=f"p99={perf['run']['p99_ms']}ms",
            metric_name="run_p99_ms",
            metric_value=perf["run"]["p99_ms"],
            threshold=PERF_RUN_P99_MS,
        )

    def block_07_chaos_resilience(self, ctx: BlockContext) -> None:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        from compliance_gate.authentication.models import Tenant
        from compliance_gate.Engine.errors import GuardrailViolation
        from compliance_gate.Engine.rulesets import (
            ClassificationMigrationPhase,
            ClassificationRuntimeMode,
            classify_records,
            ensure_baseline_ruleset_for_tenant,
            get_classification_migration_state,
            promote_classification_migration_phase,
        )
        from compliance_gate.Engine.rulesets import runtime as runtime_module
        from compliance_gate.infra.db.models import AuditLog
        from compliance_gate.infra.db.session import Base

        _, compiled = self._compile_baseline()
        rows = self._build_synthetic_records(seed=515, size=800, profile="CHAOS")

        original_check_deadline = runtime_module._check_deadline
        counter = {"value": 0}

        def flaky_deadline(*, deadline: float, row_index: int) -> None:
            counter["value"] += 1
            if counter["value"] == 40:
                raise GuardrailViolation(
                    "forced interruption",
                    details={"reason": "forced_interrupt"},
                    hint="retry",
                )
            return original_check_deadline(deadline=deadline, row_index=row_index)

        interrupted = False
        runtime_module._check_deadline = flaky_deadline
        try:
            classify_records(
                rows,
                mode=ClassificationRuntimeMode.DECLARATIVE,
                compiled_ruleset=compiled,
                context={"stale_days": 45},
            )
        except GuardrailViolation:
            interrupted = True
        finally:
            runtime_module._check_deadline = original_check_deadline
        self._record_check(
            ctx,
            check_id="chaos_forced_interruption",
            status="PASS" if interrupted else "FAIL",
            message="forced guardrail interruption captured",
        )

        recovered = classify_records(
            rows,
            mode=ClassificationRuntimeMode.DECLARATIVE,
            compiled_ruleset=compiled,
            context={"stale_days": 45},
        )
        self._record_check(
            ctx,
            check_id="chaos_retry_after_failure",
            status="PASS" if recovered.metrics.rows_classified == len(rows) else "FAIL",
            message=f"rows_classified={recovered.metrics.rows_classified}",
        )

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        session_factory = sessionmaker(bind=engine)
        db = session_factory()
        try:
            db.add(
                Tenant(
                    id="tenant-chaos",
                    slug="tenant-chaos",
                    display_name="Tenant Chaos",
                    name="Tenant Chaos",
                    is_active=True,
                )
            )
            db.commit()
            ensure_baseline_ruleset_for_tenant(db, tenant_id="tenant-chaos", actor="chaos-user")
            parity_blocked = False
            try:
                promote_classification_migration_phase(
                    db,
                    tenant_id="tenant-chaos",
                    target_phase=ClassificationMigrationPhase.B,
                    updated_by="chaos-user",
                    enforce_parity=True,
                )
            except GuardrailViolation:
                parity_blocked = True
            state_after_fail = get_classification_migration_state(db, tenant_id="tenant-chaos")
            still_a = state_after_fail.phase == ClassificationMigrationPhase.A
            promoted = promote_classification_migration_phase(
                db,
                tenant_id="tenant-chaos",
                target_phase=ClassificationMigrationPhase.B,
                updated_by="chaos-user",
                enforce_parity=False,
            )
            audit_count = (
                db.query(AuditLog)
                .filter(AuditLog.tenant_id == "tenant-chaos")
                .count()
            )
            self._record_check(
                ctx,
                check_id="chaos_partial_failure_consistency",
                status="PASS"
                if parity_blocked and still_a and promoted.phase == ClassificationMigrationPhase.B
                else "FAIL",
                message=f"parity_blocked={parity_blocked} state_after_fail={state_after_fail.phase.value} final={promoted.phase.value}",
                details={"audit_rows": audit_count},
            )
        finally:
            db.close()
            engine.dispose()

    def block_08_rbac_auth_csrf(self, ctx: BlockContext) -> None:
        checks = [
            (
                "auth_cookie_csrf_retests",
                [str(PYTHON_BIN), "retests/scripts/run_auth_retests.py"],
                2400,
            ),
            (
                "rbac_cross_tenant_retests",
                [str(PYTHON_BIN), "retests/scripts/run_rbac_retests.py"],
                3000,
            ),
        ]
        for check_id, cmd, timeout in checks:
            result = self._exec(ctx, check_id=check_id, cmd=cmd, timeout_seconds=timeout)
            self._record_check(
                ctx,
                check_id=check_id,
                status="PASS" if result.returncode == 0 else "FAIL",
                message=f"exit_code={result.returncode}",
            )

    def block_09_frontend_admin_studio_e2e(self, ctx: BlockContext) -> None:
        frontend_dir = PROJECT_ROOT / "frontend"
        tests_cmd = [
            NPM_BIN,
            "--prefix",
            "frontend",
            "run",
            "test",
            "--",
            "src/tests/engine_studio_api.test.ts",
            "src/tests/engine_studio_diagnostics.test.ts",
            "src/tests/engine_studio_main_view.integration.test.tsx",
            "src/tests/engine_studio_smoke.test.tsx",
        ]
        result_tests = self._exec(
            ctx,
            check_id="frontend_engine_studio_tests",
            cmd=tests_cmd,
            cwd=PROJECT_ROOT,
            timeout_seconds=1800,
        )
        self._record_check(
            ctx,
            check_id="frontend_engine_studio_tests",
            status="PASS" if result_tests.returncode == 0 else "FAIL",
            message=f"exit_code={result_tests.returncode}",
        )

        result_build = self._exec(
            ctx,
            check_id="frontend_build",
            cmd=[NPM_BIN, "--prefix", "frontend", "run", "build"],
            cwd=PROJECT_ROOT,
            timeout_seconds=1800,
        )
        self._record_check(
            ctx,
            check_id="frontend_build",
            status="PASS" if result_build.returncode == 0 else "FAIL",
            message=f"exit_code={result_build.returncode}",
        )

        hardcoded_scan = subprocess.run(
            ["rg", "-n", "VER STATUS", "frontend/src/main_view"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
        )
        no_hardcoded = hardcoded_scan.returncode == 1
        self._record_check(
            ctx,
            check_id="frontend_no_hardcoded_status_button",
            status="PASS" if no_hardcoded else "FAIL",
            message="hardcoded status button removed from Admin Studio",
            details={"matches": hardcoded_scan.stdout.strip()},
        )

        api_scan = subprocess.run(
            ["rg", "-n", "/api/v1/engine/views/run", "frontend/src/engine_studio"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
        )
        uses_declarative_view = api_scan.returncode == 0
        self._record_check(
            ctx,
            check_id="frontend_table_uses_declarative_view_run",
            status="PASS" if uses_declarative_view else "FAIL",
            message="engine studio uses declarative view run endpoint",
            details={"matches": api_scan.stdout.strip()},
        )

    def block_10_cutover_rollback_drills(self, ctx: BlockContext) -> None:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        from compliance_gate.authentication.models import Tenant
        from compliance_gate.Engine.rulesets import (
            ClassificationMigrationPhase,
            ClassificationRuntimeMode,
            ensure_baseline_ruleset_for_tenant,
            get_classification_migration_state,
            get_classification_mode_state,
            promote_classification_migration_phase,
            set_classification_mode,
        )
        from compliance_gate.infra.db.session import Base

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        session_factory = sessionmaker(bind=engine)
        db = session_factory()
        recovery_ms = None
        try:
            db.add(
                Tenant(
                    id="tenant-cutover",
                    slug="tenant-cutover",
                    display_name="Tenant Cutover",
                    name="Tenant Cutover",
                    is_active=True,
                )
            )
            db.commit()
            baseline = ensure_baseline_ruleset_for_tenant(
                db,
                tenant_id="tenant-cutover",
                actor="cutover-user",
            )
            ruleset_name = str(baseline["ruleset_name"])

            set_classification_mode(
                db,
                tenant_id="tenant-cutover",
                mode=ClassificationRuntimeMode.LEGACY,
                ruleset_name=None,
                updated_by="cutover-user",
            )
            set_classification_mode(
                db,
                tenant_id="tenant-cutover",
                mode=ClassificationRuntimeMode.SHADOW,
                ruleset_name=ruleset_name,
                updated_by="cutover-user",
            )
            set_classification_mode(
                db,
                tenant_id="tenant-cutover",
                mode=ClassificationRuntimeMode.DECLARATIVE,
                ruleset_name=ruleset_name,
                updated_by="cutover-user",
            )

            promote_classification_migration_phase(
                db,
                tenant_id="tenant-cutover",
                target_phase=ClassificationMigrationPhase.B,
                updated_by="cutover-user",
                enforce_parity=False,
            )
            promote_classification_migration_phase(
                db,
                tenant_id="tenant-cutover",
                target_phase=ClassificationMigrationPhase.C,
                updated_by="cutover-user",
                enforce_parity=False,
            )
            promote_classification_migration_phase(
                db,
                tenant_id="tenant-cutover",
                target_phase=ClassificationMigrationPhase.D,
                updated_by="cutover-user",
                enforce_parity=False,
            )
            state_d = get_classification_migration_state(db, tenant_id="tenant-cutover")

            start = time.perf_counter()
            set_classification_mode(
                db,
                tenant_id="tenant-cutover",
                mode=ClassificationRuntimeMode.LEGACY,
                ruleset_name=None,
                updated_by="cutover-user",
            )
            recovery_ms = round((time.perf_counter() - start) * 1000.0, 4)
            mode_state = get_classification_mode_state(db, tenant_id="tenant-cutover")

            pass_cutover = (
                state_d.phase == ClassificationMigrationPhase.D
                and mode_state.mode == ClassificationRuntimeMode.LEGACY
            )
            self._record_check(
                ctx,
                check_id="cutover_to_declarative_only_and_rollback",
                status="PASS" if pass_cutover else "FAIL",
                message=f"phase={state_d.phase.value} mode_after_rollback={mode_state.mode.value}",
                metric_name="rollback_recovery_ms",
                metric_value=recovery_ms,
                threshold=5000,
            )
            self.global_metrics["rollback"] = {
                "final_phase": state_d.phase.value,
                "final_mode": mode_state.mode.value,
                "recovery_ms": recovery_ms,
                "ruleset_name": ruleset_name,
            }
            ctx.details["rollback"] = self.global_metrics["rollback"]
        finally:
            db.close()
            engine.dispose()

    def block_11_mutation_testing(self, ctx: BlockContext) -> None:
        from compliance_gate.Engine.rulesets import (
            ClassificationRuntimeMode,
            RuleSetPayloadV2,
            classify_records,
            compile_ruleset_from_payload,
        )

        baseline_payload, baseline_compiled = self._compile_baseline()
        rows = self._build_synthetic_records(seed=1212, size=3200, profile="MUT")

        baseline_shadow = classify_records(
            rows,
            mode=ClassificationRuntimeMode.SHADOW,
            compiled_ruleset=baseline_compiled,
            context={"stale_days": 45},
        )
        baseline_div = len(baseline_shadow.divergences)

        payload_dict = baseline_payload.model_dump(mode="json")
        mutated = False
        for block in payload_dict.get("blocks", []):
            if block.get("kind") != "primary":
                continue
            for entry in block.get("entries", []):
                if entry.get("rule_key") != "primary_rogue":
                    continue
                clauses = entry.get("condition", {}).get("clauses", [])
                if len(clauses) >= 3:
                    target = clauses[2].get("right")
                    if isinstance(target, dict):
                        target["value"] = True
                        mutated = True
                        break
            if mutated:
                break

        self._record_check(
            ctx,
            check_id="mutation_applied",
            status="PASS" if mutated else "FAIL",
            message="primary_rogue mutation toggled has_edr literal",
        )

        mutated_payload = RuleSetPayloadV2.model_validate(payload_dict)
        mutated_compiled = compile_ruleset_from_payload(
            mutated_payload,
            ruleset_name="mutated",
            version=2,
        )
        mutated_shadow = classify_records(
            rows,
            mode=ClassificationRuntimeMode.SHADOW,
            compiled_ruleset=mutated_compiled,
            context={"stale_days": 45},
        )
        mutated_div = len(mutated_shadow.divergences)
        detection = mutated_div > baseline_div
        self._record_check(
            ctx,
            check_id="mutation_detected_by_suite",
            status="PASS" if detection else "FAIL",
            message=f"baseline_divergences={baseline_div} mutated_divergences={mutated_div}",
            details={"baseline_divergences": baseline_div, "mutated_divergences": mutated_div},
        )
        ctx.details["mutation"] = {
            "baseline_divergences": baseline_div,
            "mutated_divergences": mutated_div,
        }

    def block_12_ai_readiness_contract(self, ctx: BlockContext) -> None:
        from compliance_gate.Engine.errors import UnknownColumn
        from compliance_gate.Engine.rulesets import explain_row, validate_ruleset_payload

        payload, compiled = self._compile_baseline()
        payload_dump = payload.model_dump(mode="json")
        dsl_ok = (
            isinstance(payload_dump.get("schema_version"), int)
            and isinstance(payload_dump.get("blocks"), list)
            and all(isinstance(block.get("entries"), list) for block in payload_dump.get("blocks", []))
        )
        self._record_check(
            ctx,
            check_id="ai_dsl_stable_shape",
            status="PASS" if dsl_ok else "FAIL",
            message=f"schema_version={payload_dump.get('schema_version')} blocks={len(payload_dump.get('blocks', []))}",
        )

        row = self._build_synthetic_records(seed=8181, size=1, profile="AI")[0]
        explain_1 = explain_row(compiled, row=row)
        explain_2 = explain_row(compiled, row=row)
        deterministic = json.dumps(explain_1, sort_keys=True, ensure_ascii=False) == json.dumps(
            explain_2, sort_keys=True, ensure_ascii=False
        )
        self._record_check(
            ctx,
            check_id="ai_explain_deterministic",
            status="PASS" if deterministic else "FAIL",
            message=f"matched_rules={explain_1.get('matched_rules')}",
            details={
                "evaluation_order": explain_1.get("evaluation_order"),
                "decision_reason": explain_1.get("decision_reason"),
            },
        )

        invalid_payload = self._payload_with_single_rule(
            {
                "node_type": "binary_op",
                "operator": "==",
                "left": {"node_type": "column_ref", "column": "hostnme"},
                "right": {"node_type": "literal", "value_type": "string", "value": "X"},
            },
            {"primary_status": "COMPLIANT"},
        )
        validation = validate_ruleset_payload(invalid_payload)
        issue = validation.get("issues", [{}])[0]
        stable_issue_shape = {"code", "message", "details", "hint", "node_path", "stage", "severity"}.issubset(
            issue.keys()
        )
        self._record_check(
            ctx,
            check_id="ai_error_payload_stable",
            status="PASS" if stable_issue_shape else "FAIL",
            message=f"issue_code={issue.get('code')}",
            details=issue,
        )

        error_payload = UnknownColumn(
            details={"column": "hostnme", "suggestions": ["hostname"]},
            node_path="root.left",
        ).to_dict()
        error_contract = {"code", "message", "details", "hint", "node_path"}.issubset(
            error_payload.keys()
        )
        self._record_check(
            ctx,
            check_id="ai_exception_contract_stable",
            status="PASS" if error_contract else "FAIL",
            message=f"keys={sorted(error_payload.keys())}",
            details=error_payload,
        )
        ctx.details["ai_contract"] = {
            "validation_issue": issue,
            "error_payload": error_payload,
        }

    def _write_artifacts(self, certified: bool) -> None:
        matrix_header = [
            "run_id",
            "block_id",
            "block_name",
            "check_id",
            "status",
            "severity",
            "message",
            "metric_name",
            "metric_value",
            "threshold",
            "details_json",
        ]
        with self.matrix_csv_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(matrix_header)
            for row in self.matrix:
                writer.writerow(
                    [
                        self.run_id,
                        row.block_id,
                        row.block_name,
                        row.check_id,
                        row.status,
                        row.severity,
                        row.message,
                        row.metric_name or "",
                        row.metric_value or "",
                        row.threshold or "",
                        json.dumps(_safe_json(row.details), ensure_ascii=False),
                    ]
                )

        self.failures_json_path.write_text(
            json.dumps(_safe_json(self.failures), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        payload = {
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "ended_at": _utc_now().isoformat(),
            "status": "CERTIFIED" if certified else "NOT CERTIFIED",
            "parity_threshold": PARITY_THRESHOLD,
            "blocks": [
                {
                    "block_id": block.block_id,
                    "block_name": block.block_name,
                    "status": block.status,
                    "duration_ms": block.duration_ms,
                    "checks_total": block.checks_total,
                    "checks_passed": block.checks_passed,
                    "checks_failed": block.checks_failed,
                    "log_path": block.log_path,
                    "details": block.details,
                    "failures": block.failures,
                    "commands": [
                        {
                            "check_id": cmd.check_id,
                            "command": cmd.command,
                            "cwd": cmd.cwd,
                            "exit_code": cmd.exit_code,
                            "duration_ms": cmd.duration_ms,
                            "log_path": cmd.log_path,
                            "timeout_seconds": cmd.timeout_seconds,
                        }
                        for cmd in block.commands
                    ],
                }
                for block in self.block_results
            ],
            "matrix_path": str(self.matrix_csv_path),
            "failures_path": str(self.failures_json_path),
            "report_path": str(self.report_path),
            "global_metrics": _safe_json(self.global_metrics),
        }
        self.run_json_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        total_checks = len(self.matrix)
        failed_checks = len([row for row in self.matrix if row.status != "PASS"])
        parity = self.global_metrics.get("parity", {})
        performance = self.global_metrics.get("performance", {})
        rollback = self.global_metrics.get("rollback", {})
        min_parity = parity.get("minimum_percent")
        min_parity_text = f"{min_parity:.4f}%" if isinstance(min_parity, (int, float)) else "n/a"
        go_no_go = "GO" if certified else "NO-GO"

        residual_risks: list[str] = []
        blockers: list[str] = []
        if min_parity is None or float(min_parity) < PARITY_THRESHOLD:
            residual_risks.append(
                f"Parity below threshold ({min_parity_text} < {PARITY_THRESHOLD:.4f}%)."
            )
            blockers.append("Legacy parity gate not satisfied.")
        if failed_checks > 0:
            residual_risks.append(f"{failed_checks} check(s) failed in the ultimate matrix.")
            blockers.append("At least one mandatory block failed.")
        if not rollback:
            residual_risks.append("Rollback drill evidence is missing.")
            blockers.append("Rollback drill not proven.")

        if not residual_risks:
            residual_risks.append("No critical residual risk identified in this run.")
        if not blockers:
            blockers.append("None.")

        block_table = [
            "| Block | Status | Checks (pass/total) | Log |",
            "|---|---|---|---|",
        ]
        for block in self.block_results:
            block_table.append(
                f"| {block.block_id} - {block.block_name} | {block.status} | {block.checks_passed}/{block.checks_total} | `{block.log_path}` |"
            )

        failures_preview = self.failures[:15]
        failure_lines = []
        if failures_preview:
            for failure in failures_preview:
                failure_lines.append(
                    f"- [{failure['block_id']}/{failure['check_id']}] {failure['message']}"
                )
        else:
            failure_lines.append("- None.")

        perf_lines = []
        if performance:
            perf_lines.append(
                f"- Classification p50/p95/p99 (ms): {performance['classification']['p50_ms']} / {performance['classification']['p95_ms']} / {performance['classification']['p99_ms']}"
            )
            perf_lines.append(
                f"- Preview p50/p95/p99 (ms): {performance['preview']['p50_ms']} / {performance['preview']['p95_ms']} / {performance['preview']['p99_ms']}"
            )
            perf_lines.append(
                f"- Run p50/p95/p99 (ms): {performance['run']['p50_ms']} / {performance['run']['p95_ms']} / {performance['run']['p99_ms']}"
            )
        else:
            perf_lines.append("- Performance metrics not available.")

        report_lines = [
            "# ENGINE ULTIMATE CERTIFICATION REPORT",
            "",
            f"STATUS: {'CERTIFIED' if certified else 'NOT CERTIFIED'}",
            "",
            "## Certification Summary",
            f"- Run ID: `{self.run_id}`",
            f"- Started (UTC): `{self.started_at.isoformat()}`",
            f"- Finished (UTC): `{_utc_now().isoformat()}`",
            f"- Total checks: `{total_checks}`",
            f"- Failed checks: `{failed_checks}`",
            f"- Parity threshold: `{PARITY_THRESHOLD:.4f}%`",
            f"- Minimum observed parity: `{min_parity_text}`",
            f"- Decision (signable): `{go_no_go}`",
            "",
            "## Coverage Matrix",
            *block_table,
            "",
            "## Metrics",
            *perf_lines,
            "",
            "## Parity and Divergence Evidence",
            f"- Parity datasets: `{json.dumps(_safe_json(parity.get('datasets', [])), ensure_ascii=False)}`",
            f"- Divergence by kind: `{json.dumps(_safe_json(parity.get('by_kind', {})), ensure_ascii=False)}`",
            f"- Divergence by severity: `{json.dumps(_safe_json(parity.get('by_severity', {})), ensure_ascii=False)}`",
            "",
            "## Rollback Drill Evidence",
            f"- Rollback metrics: `{json.dumps(_safe_json(rollback), ensure_ascii=False)}`",
            "",
            "## Human Error UX Evidence",
            f"- Error contract sample: `{json.dumps(_safe_json(self.global_metrics.get('error_contract', {})), ensure_ascii=False)}`",
            "",
            "## Remaining Risks",
            *[f"- {risk}" for risk in residual_risks],
            "",
            "## Remaining Blockers",
            *[f"- {item}" for item in blockers],
            "",
            "## Failed Checks (first 15)",
            *failure_lines,
            "",
            "## Artifact Paths",
            f"- Run JSON: `{self.run_json_path}`",
            f"- Matrix CSV: `{self.matrix_csv_path}`",
            f"- Failures JSON: `{self.failures_json_path}`",
            f"- Block logs dir: `{self.block_logs_dir}`",
            "",
            "## Objective Next Steps",
            "1. Fix every failed matrix check and re-run this orchestrator.",
            "2. Reconfirm parity gate on production-like datasets (>= threshold) before declarative-only.",
            "3. Run rollback drill weekly while cutover is in progress.",
            "",
        ]
        self.report_path.write_text("\n".join(report_lines), encoding="utf-8")

    def run(self) -> int:
        self._run_block("01", "Unit/Contract/Migration", self.block_01_unit_contract_migration)
        self._run_block("02", "Runtime Determinism", self.block_02_runtime_determinism)
        self._run_block("03", "Legacy Parity", self.block_03_legacy_parity)
        self._run_block("04", "Error Robustness", self.block_04_error_robustness)
        self._run_block("05", "Guardrails and Security", self.block_05_guardrails_security)
        self._run_block("06", "Performance and Scale", self.block_06_performance_scale)
        self._run_block("07", "Chaos and Resilience", self.block_07_chaos_resilience)
        self._run_block("08", "RBAC/Auth/CSRF", self.block_08_rbac_auth_csrf)
        self._run_block("09", "Frontend Admin Studio E2E", self.block_09_frontend_admin_studio_e2e)
        self._run_block("10", "Cutover and Rollback Drills", self.block_10_cutover_rollback_drills)
        self._run_block("11", "Mutation Testing", self.block_11_mutation_testing)
        self._run_block("12", "AI-Readiness Contract", self.block_12_ai_readiness_contract)

        all_passed = all(block.status == "PASS" for block in self.block_results)
        parity = self.global_metrics.get("parity", {})
        parity_ok = float(parity.get("minimum_percent", 0.0)) >= PARITY_THRESHOLD
        certified = all_passed and parity_ok
        self._write_artifacts(certified=certified)

        print(f"run_id={self.run_id}")
        print(f"report={self.report_path}")
        print(f"run_json={self.run_json_path}")
        print(f"matrix_csv={self.matrix_csv_path}")
        print(f"failures_json={self.failures_json_path}")
        print(f"status={'CERTIFIED' if certified else 'NOT CERTIFIED'}")
        return 0 if certified else 2


def main() -> int:
    runner = UltimateRetestRunner()
    return runner.run()


if __name__ == "__main__":
    raise SystemExit(main())
