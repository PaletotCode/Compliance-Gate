"""
run_retests.py — Compliance Gate Isolated Retest Orchestrator

Sections:
  A) Pre-check       — verify CSV files exist
  B) CSV Read        — tolerant reading with auto-delimiter, auto-encoding, dynamic header
  C) Export          — head.csv, schema.json, profile.json per source
  D) Validate        — structural validation vs expected_headers.json
  E) Endpoints       — test FastAPI endpoints (optional, skipped if api unreachable)
  F) Report          — generate report_<run_id>.md

Exit codes:
  0 → success (warnings allowed)
  1 → critical failure (missing files or unreadable CSVs)
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import requests
try:
    from rich.console import Console
    from rich.table import Table
except Exception:  # pragma: no cover - optional dependency in local retests
    class Console:
        def print(self, *args, **kwargs):
            print(*args)

    class Table:
        def __init__(self, title: str | None = None, **_kwargs):
            self.title = title or ""

        def add_column(self, *_args, **_kwargs):
            return None

        def add_row(self, *_args, **_kwargs):
            return None

        def __str__(self) -> str:
            return f"[table] {self.title}"


# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────

WORKSPACE = Path(os.environ.get("WORKSPACE", "/workspace"))
API_BASE_URL = os.environ.get("API_BASE_URL", "http://api:8000")
API_PREFIX = "/api/v1"
AUTH_PREFIX = f"{API_PREFIX}/auth"
RUN_ENDPOINT_TESTS = os.environ.get("RUN_ENDPOINT_TESTS", "true").lower() == "true"
BOOTSTRAP_ADMIN_USERNAME = os.environ.get("AUTH_BOOTSTRAP_ADMIN_USERNAME", "admin")
BOOTSTRAP_ADMIN_PASSWORD = os.environ.get("AUTH_BOOTSTRAP_ADMIN_PASSWORD", "Admin1234")
AUTH_COOKIE_NAME = os.environ.get("AUTH_COOKIE_NAME", "cg_access")
CSRF_COOKIE_NAME = os.environ.get("CSRF_COOKIE_NAME", "cg_csrf")
CSRF_HEADER_NAME = os.environ.get("CSRF_HEADER_NAME", "X-CSRF-Token")
PROJECT_ROOT = Path(__file__).resolve().parents[2]

RETESTS_DIR = Path(os.environ.get("RETESTS_DIR", str(PROJECT_ROOT / "retests")))
LOGS_DIR = RETESTS_DIR / "logs"
OUTPUT_DIR = RETESTS_DIR / "output"
SCRIPTS_DIR = RETESTS_DIR / "scripts"
EXPECTED_HEADERS_FILE = SCRIPTS_DIR / "expected_headers.json"

CSV_FILES = {
    "ASSET": WORKSPACE / "ASSET.CSV",
    "UEM": WORKSPACE / "UEM.csv",
    "AD": WORKSPACE / "AD.csv",
    "EDR": WORKSPACE / "EDR.csv",
}

ENDPOINTS = {
    "health": "/health",
    "version": "/version",
    "machines_filters": f"{API_PREFIX}/machines/filters",
    "machines_summary": f"{API_PREFIX}/machines/summary",
    "machines_table": f"{API_PREFIX}/machines/table?page=1&page_size=50",
    "datasets_list": f"{API_PREFIX}/datasets/machines",
}

API_WAIT_RETRIES = 10
API_WAIT_SLEEP = 5  # seconds between retries

# ──────────────────────────────────────────────────────────────────────────────
# Run ID generation
# ──────────────────────────────────────────────────────────────────────────────

def make_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    short = hashlib.sha1(ts.encode()).hexdigest()[:6]
    return f"{ts}_{short}"


RUN_ID = make_run_id()
LOG_FILE = LOGS_DIR / f"run_{RUN_ID}.log"

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────

console = Console()

_log_fh = None

def _ensure_log():
    global _log_fh
    if _log_fh is None:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        _log_fh = open(LOG_FILE, "w", encoding="utf-8")


def log(level: str, section: str, msg: str):
    _ensure_log()
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    line = f"[{ts}] [{level:5s}] [{section}] {msg}"
    color_map = {"INFO": "cyan", "OK": "green", "WARN": "yellow", "ERROR": "red", "STEP": "bold blue"}
    console.print(line, style=color_map.get(level, "white"))
    _log_fh.write(line + "\n")
    _log_fh.flush()


def log_separator(section: str):
    bar = "─" * 70
    log("STEP", section, bar)


# ──────────────────────────────────────────────────────────────────────────────
# Result accumulator
# ──────────────────────────────────────────────────────────────────────────────

results: dict[str, Any] = {
    "run_id": RUN_ID,
    "started_at": datetime.now(timezone.utc).isoformat(),
    "files": {},
    "dataframes": {},
    "validation": {},
    "endpoints": {},
    "problems": [],  # list of {"severity": "WARN|ERROR", "source": str, "msg": str}
    "outputs": [],
    "recommendations": [],
}

_api_session: requests.Session | None = None
_api_auth_attempted = False


def add_problem(severity: str, source: str, msg: str):
    results["problems"].append({"severity": severity, "source": source, "msg": msg})
    log(severity, source, msg)


def _get_authenticated_session() -> requests.Session | None:
    global _api_session
    global _api_auth_attempted

    if _api_auth_attempted:
        return _api_session

    _api_auth_attempted = True
    try:
        session = requests.Session()
        response = session.post(
            f"{API_BASE_URL}{AUTH_PREFIX}/login",
            json={"username": BOOTSTRAP_ADMIN_USERNAME, "password": BOOTSTRAP_ADMIN_PASSWORD},
            timeout=15,
        )
        if response.status_code != 200:
            add_problem(
                "WARN",
                "AUTH",
                f"Could not authenticate retest session: {response.status_code} {response.text}",
            )
            return None
        payload = response.json()
        if payload.get("mfa_required"):
            add_problem("WARN", "AUTH", "Bootstrap admin requires MFA; retests will use anonymous requests.")
            return None
        if not session.cookies.get(AUTH_COOKIE_NAME):
            add_problem("WARN", "AUTH", "Auth login did not set auth cookie; retests will use anonymous requests.")
            return None
        _api_session = session
        log("OK", "AUTH", "Authenticated API session acquired for endpoint retests.")
        return _api_session
    except Exception as exc:
        add_problem("WARN", "AUTH", f"Failed to authenticate retest session: {exc}")
        return None


def _api_request(
    method: str,
    url: str,
    *,
    json_body: dict | None = None,
    timeout: int = 15,
) -> requests.Response:
    session = _get_authenticated_session()
    headers: dict[str, str] = {}

    if method.upper() in {"POST", "PUT", "PATCH", "DELETE"} and session is not None:
        csrf_token = session.cookies.get(CSRF_COOKIE_NAME)
        if csrf_token:
            headers[CSRF_HEADER_NAME] = csrf_token

    if session is not None:
        return session.request(method, url, json=json_body, headers=headers, timeout=timeout)
    return requests.request(method, url, json=json_body, headers=headers, timeout=timeout)


# ──────────────────────────────────────────────────────────────────────────────
# Section A — Pre-check
# ──────────────────────────────────────────────────────────────────────────────

def section_a() -> bool:
    log_separator("A:PRE-CHECK")
    log("STEP", "A:PRE-CHECK", f"Workspace: {WORKSPACE}")
    ok = True
    for name, path in CSV_FILES.items():
        if path.exists():
            size = path.stat().st_size
            results["files"][name] = {"path": str(path), "size_bytes": size}
            log("OK", "A:PRE-CHECK", f"{name}: {path.name}  ({size:,} bytes)")
        else:
            # Try case-insensitive fallback
            parent = path.parent
            matches = list(parent.glob(f"*{path.suffix}")) if parent.exists() else []
            found = next((m for m in matches if m.name.upper() == path.name.upper()), None)
            if found:
                size = found.stat().st_size
                CSV_FILES[name] = found
                results["files"][name] = {"path": str(found), "size_bytes": size}
                log("WARN", "A:PRE-CHECK", f"{name}: found with different case → {found.name}  ({size:,} bytes)")
            else:
                results["files"][name] = {"path": str(path), "size_bytes": 0, "missing": True}
                add_problem("ERROR", "A:PRE-CHECK", f"{name}: FILE NOT FOUND at {path}")
                ok = False
    return ok


# ──────────────────────────────────────────────────────────────────────────────
# Section B — Robust CSV reading
# ──────────────────────────────────────────────────────────────────────────────

def _try_read_polars(path: Path, sep: str, encoding: str, skip_rows: int) -> pl.DataFrame | None:
    try:
        df = pl.read_csv(
            path,
            separator=sep,
            encoding=encoding,
            skip_rows=skip_rows,
            infer_schema_length=500,
            ignore_errors=True,
            truncate_ragged_lines=True,
        )
        if df.shape[0] == 0:
            return None
        return df
    except Exception:
        return None


def _find_asset_header_row(path: Path, encoding: str) -> int:
    """Scan ASSET.CSV rows until we find one containing 'NOME DO ATIVO'."""
    try:
        with open(path, "r", encoding=encoding, errors="replace") as f:
            for i, line in enumerate(f):
                if "NOME DO ATIVO" in line.upper():
                    return i  # 0-based
    except Exception:
        pass
    return 4  # fallback: row 4 (0-based) = line 5


def read_csv_tolerant(name: str, path: Path) -> pl.DataFrame | None:
    log("STEP", f"B:READ:{name}", f"Reading {path.name} …")
    separators = [",", ";", "\t"]
    encodings = ["utf-8-sig", "utf-8", "latin-1", "cp1252"]

    skip_rows = 0
    if name == "ASSET":
        # Dynamic header — scan for the actual header row
        for enc in encodings:
            row_idx = _find_asset_header_row(path, enc)
            if row_idx > 0:
                log("INFO", f"B:READ:{name}", f"Dynamic header detected at row {row_idx + 1} (0-based: {row_idx}), encoding probe: {enc}")
                skip_rows = row_idx
                break

    for enc in encodings:
        for sep in separators:
            df = _try_read_polars(path, sep, enc, skip_rows)
            if df is not None and df.shape[1] > 1:
                log("OK", f"B:READ:{name}", f"Read OK — sep='{sep}' enc='{enc}' rows={df.shape[0]:,} cols={df.shape[1]}")
                return df

    add_problem("ERROR", f"B:READ:{name}", "Could not parse CSV with any sep/encoding combination.")
    return None


def section_b() -> dict[str, pl.DataFrame | None]:
    log_separator("B:CSV-READ")
    frames: dict[str, pl.DataFrame | None] = {}
    for name, path in CSV_FILES.items():
        if results["files"].get(name, {}).get("missing"):
            frames[name] = None
            continue
        t0 = time.perf_counter()
        df = read_csv_tolerant(name, path)
        elapsed = time.perf_counter() - t0

        if df is None:
            frames[name] = None
            results["dataframes"][name] = {"error": "parse_failed"}
            continue

        null_counts = {col: df[col].null_count() for col in df.columns}
        results["dataframes"][name] = {
            "rows": df.shape[0],
            "cols": df.shape[1],
            "columns": list(df.columns),
            "dtypes": {col: str(df[col].dtype) for col in df.columns},
            "null_counts": null_counts,
            "read_elapsed_s": round(elapsed, 3),
        }
        log("INFO", f"B:READ:{name}", f"Rows: {df.shape[0]:,} | Cols: {df.shape[1]} | Time: {elapsed:.3f}s")
        frames[name] = df

    return frames


# ──────────────────────────────────────────────────────────────────────────────
# Section C — Export artifacts
# ──────────────────────────────────────────────────────────────────────────────

def export_head(name: str, df: pl.DataFrame) -> Path:
    out = OUTPUT_DIR / f"{name}_head.csv"
    df.head(20).write_csv(out)
    return out


def export_schema(name: str, df: pl.DataFrame) -> Path:
    schema = {col: str(df[col].dtype) for col in df.columns}
    out = OUTPUT_DIR / f"{name}_schema.json"
    out.write_text(json.dumps(schema, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def export_profile(name: str, df: pl.DataFrame) -> Path:
    profile: dict[str, Any] = {
        "rows": df.shape[0],
        "columns": df.shape[1],
        "column_profiles": {},
    }
    for col in df.columns:
        series = df[col]
        col_p: dict[str, Any] = {
            "dtype": str(series.dtype),
            "null_count": series.null_count(),
            "null_pct": round(series.null_count() / max(df.shape[0], 1) * 100, 2),
            "unique": series.n_unique(),
        }
        if series.dtype in (pl.Int32, pl.Int64, pl.Float32, pl.Float64):
            try:
                col_p["min"] = series.min()
                col_p["max"] = series.max()
                col_p["mean"] = round(series.mean() or 0.0, 4)
            except Exception:
                pass
        elif series.dtype == pl.Utf8 or series.dtype == pl.String:
            try:
                non_null = series.drop_nulls()
                if non_null.len() > 0:
                    col_p["sample_values"] = non_null.head(5).to_list()
                    col_p["min_len"] = int(non_null.str.len_chars().min() or 0)
                    col_p["max_len"] = int(non_null.str.len_chars().max() or 0)
            except Exception:
                pass
        profile["column_profiles"][col] = col_p

    out = OUTPUT_DIR / f"{name}_profile.json"
    out.write_text(json.dumps(profile, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return out


def section_c(frames: dict[str, pl.DataFrame | None]):
    log_separator("C:EXPORT")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, df in frames.items():
        if df is None:
            log("WARN", f"C:EXPORT:{name}", "Skipped — no DataFrame.")
            continue
        h = export_head(name, df)
        s = export_schema(name, df)
        p = export_profile(name, df)
        results["outputs"].extend([str(h), str(s), str(p)])
        log("OK", f"C:EXPORT:{name}", f"head → {h.name} | schema → {s.name} | profile → {p.name}")

        # Rich table preview (first 5 rows, first 6 cols)
        show_cols = list(df.columns)[:6]
        rtable = Table(title=f"{name} — head (5 rows, first 6 cols)", show_lines=True)
        for c in show_cols:
            rtable.add_column(c, overflow="fold", max_width=30)
        for row in df.head(5).rows():
            rtable.add_row(*[str(v) if v is not None else "" for v in row[:6]])
        console.print(rtable)


# ──────────────────────────────────────────────────────────────────────────────
# Section D — Structural validation
# ──────────────────────────────────────────────────────────────────────────────

def section_d(frames: dict[str, pl.DataFrame | None]):
    log_separator("D:VALIDATE")
    if not EXPECTED_HEADERS_FILE.exists():
        add_problem("WARN", "D:VALIDATE", f"expected_headers.json not found at {EXPECTED_HEADERS_FILE}. Skipping column validation.")
        return

    expected: dict[str, Any] = json.loads(EXPECTED_HEADERS_FILE.read_text(encoding="utf-8"))

    for name, df in frames.items():
        if df is None:
            add_problem("WARN", f"D:VALIDATE:{name}", "DataFrame missing — skipping validation.")
            continue

        spec = expected.get(name, {})
        required_cols = spec.get("required", [])
        optional_cols = spec.get("optional", [])
        aliases: dict[str, list[str]] = spec.get("aliases", {})

        actual_cols_upper = {c.upper(): c for c in df.columns}

        found_required = []
        missing_required = []
        via_alias: dict[str, str] = {}

        for req in required_cols:
            if req.upper() in actual_cols_upper:
                found_required.append(req)
            else:
                # Check aliases
                alt_list = aliases.get(req, [])
                found_alt = next(
                    (alt for alt in alt_list if alt.upper() in actual_cols_upper), None
                )
                if found_alt:
                    found_required.append(req)
                    via_alias[req] = found_alt
                else:
                    missing_required.append(req)

        # Check for optional missing (WARN only)
        missing_optional = [
            opt for opt in optional_cols
            if opt.upper() not in actual_cols_upper
        ]

        # Check for empty headers
        empty_headers = [c for c in df.columns if not c.strip()]
        # Check for duplicate headers
        dup_headers = [c for c in df.columns if df.columns.count(c) > 1]

        val_result = {
            "found_required": found_required,
            "missing_required": missing_required,
            "missing_optional": missing_optional,
            "via_alias": via_alias,
            "empty_headers": empty_headers,
            "duplicate_headers": list(set(dup_headers)),
        }
        results["validation"][name] = val_result

        if via_alias:
            log("INFO", f"D:VALIDATE:{name}", f"Required cols found via alias: {via_alias}")

        for col in missing_required:
            add_problem("ERROR", f"D:VALIDATE:{name}", f"MISSING REQUIRED column: '{col}'")
            results["recommendations"].append(
                f"[{name}] Required column '{col}' not found. Check the CSV export settings or update expected_headers.json with the correct alias."
            )

        if missing_optional:
            log("WARN", f"D:VALIDATE:{name}", f"Optional columns not present: {missing_optional}")
            spec_notes = spec.get("notes", "")
            if any("Last Logon Time" in m or "Password Last Set" in m for m in missing_optional):
                results["recommendations"].append(
                    f"[{name}] '{', '.join(missing_optional)}' are referenced by dashboard_fixed.ts but missing in the current export. "
                    f"Re-export AD with those columns if logon/password date data is needed. Non-blocking for current pipeline."
                )

        if empty_headers:
            add_problem("WARN", f"D:VALIDATE:{name}", f"Empty column headers detected: {empty_headers}")

        if dup_headers:
            add_problem("WARN", f"D:VALIDATE:{name}", f"Duplicate column names detected: {list(set(dup_headers))}")

        if not missing_required and not empty_headers and not dup_headers:
            log("OK", f"D:VALIDATE:{name}", "All required columns present. Validation PASSED.")
        elif missing_required:
            log("ERROR", f"D:VALIDATE:{name}", f"Validation FAILED — {len(missing_required)} required column(s) missing.")
        else:
            log("WARN", f"D:VALIDATE:{name}", "Validation passed with warnings.")


# ──────────────────────────────────────────────────────────────────────────────
# Section E — Endpoint tests
# ──────────────────────────────────────────────────────────────────────────────

def _wait_for_api() -> bool:
    log("INFO", "E:ENDPOINTS", f"Waiting for API at {API_BASE_URL} …")
    for attempt in range(1, API_WAIT_RETRIES + 1):
        try:
            r = requests.get(f"{API_BASE_URL}/health", timeout=4)
            if r.status_code == 200:
                log("OK", "E:ENDPOINTS", f"API is ready (attempt {attempt})")
                return True
        except Exception:
            pass
        log("INFO", "E:ENDPOINTS", f"  Attempt {attempt}/{API_WAIT_RETRIES} — waiting {API_WAIT_SLEEP}s …")
        time.sleep(API_WAIT_SLEEP)
    return False


def section_e():
    log_separator("E:ENDPOINTS")
    if not RUN_ENDPOINT_TESTS:
        log("INFO", "E:ENDPOINTS", "RUN_ENDPOINT_TESTS=false — skipping endpoint tests.")
        return

    api_ready = _wait_for_api()
    if not api_ready:
        add_problem("WARN", "E:ENDPOINTS", f"API not reachable at {API_BASE_URL} after {API_WAIT_RETRIES} retries. Skipping endpoint tests.")
        results["recommendations"].append(
            "API was unreachable during retests. Ensure db/redis are healthy and the api container starts correctly. "
            "Re-run with RUN_ENDPOINT_TESTS=true once the API is up."
        )
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for key, path in ENDPOINTS.items():
        url = f"{API_BASE_URL}{path}"
        t0 = time.perf_counter()
        try:
            response = _api_request("GET", url, timeout=15)
            elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
            status = response.status_code
            try:
                body = response.json()
            except Exception:
                body = {"raw": response.text[:500]}

            out_path = OUTPUT_DIR / f"api_{key}_{RUN_ID}.json"
            out_path.write_text(
                json.dumps({"url": url, "status": status, "elapsed_ms": elapsed_ms, "body": body},
                           ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )
            results["outputs"].append(str(out_path))
            results["endpoints"][key] = {"url": url, "status": status, "elapsed_ms": elapsed_ms}

            if status == 200:
                log("OK", f"E:ENDPOINTS:{key}", f"HTTP {status}  {elapsed_ms}ms → {out_path.name}")
            else:
                add_problem("WARN", f"E:ENDPOINTS:{key}", f"HTTP {status}  {elapsed_ms}ms  url={url}")

        except Exception as exc:
            elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
            results["endpoints"][key] = {"url": url, "status": "ERROR", "elapsed_ms": elapsed_ms, "error": str(exc)}
            add_problem("ERROR", f"E:ENDPOINTS:{key}", f"Request failed: {exc}")


# ──────────────────────────────────────────────────────────────────────────────
# Section G — Dataset pipeline: preview → ingest → versioned summary
# ──────────────────────────────────────────────────────────────────────────────

def section_g():
    log_separator("G:DATASET-PIPELINE")
    if not RUN_ENDPOINT_TESTS:
        log("INFO", "G:DATASET-PIPELINE", "RUN_ENDPOINT_TESTS=false — skipping dataset pipeline tests.")
        return

    workspace_str = str(WORKSPACE)

    # ── G1: Preview (dry-run) ────────────────────────────────────────────────
    preview_url = f"{API_BASE_URL}{API_PREFIX}/datasets/machines/preview"
    log("STEP", "G:PREVIEW", f"POST {preview_url}")
    t0 = time.perf_counter()
    try:
        r = _api_request("POST", preview_url, json_body={"data_dir": workspace_str}, timeout=60)
        elapsed = round((time.perf_counter() - t0) * 1000, 1)
        body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        out_path = OUTPUT_DIR / f"api_preview_{RUN_ID}.json"
        out_path.write_text(json.dumps({"status": r.status_code, "body": body}, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        results["outputs"].append(str(out_path))
        if r.status_code == 200:
            log("OK", "G:PREVIEW", f"HTTP {r.status_code}  {elapsed}ms  layouts={len(body.get('layouts', []))}")
        else:
            add_problem("WARN", "G:PREVIEW", f"HTTP {r.status_code}  url={preview_url}")
    except Exception as exc:
        add_problem("ERROR", "G:PREVIEW", f"Request failed: {exc}")
        return

    # ── G2: Ingest (persists dataset_version) ────────────────────────────────
    ingest_url = f"{API_BASE_URL}{API_PREFIX}/datasets/machines/ingest"
    log("STEP", "G:INGEST", f"POST {ingest_url}")
    t0 = time.perf_counter()
    dataset_version_id = None
    try:
        r = _api_request(
            "POST",
            ingest_url,
            json_body={"source": "path", "data_dir": workspace_str},
            timeout=120,
        )
        elapsed = round((time.perf_counter() - t0) * 1000, 1)
        body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        out_path = OUTPUT_DIR / f"api_ingest_{RUN_ID}.json"
        out_path.write_text(json.dumps({"status": r.status_code, "body": body}, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        results["outputs"].append(str(out_path))
        if r.status_code == 200:
            dataset_version_id = body.get("dataset_version_id")
            total = body.get("total_records", "?")
            log("OK", "G:INGEST", f"HTTP {r.status_code}  {elapsed}ms  records={total}  version_id={dataset_version_id}")
        else:
            add_problem("WARN", "G:INGEST", f"HTTP {r.status_code}  url={ingest_url}")
    except Exception as exc:
        add_problem("ERROR", "G:INGEST", f"Request failed: {exc}")
        return

    # ── G3: Versioned summary ─────────────────────────────────────────────────
    if dataset_version_id:
        summary_url = f"{API_BASE_URL}{API_PREFIX}/machines/summary?dataset_version_id={dataset_version_id}"
        log("STEP", "G:SUMMARY-VERSIONED", f"GET {summary_url}")
        t0 = time.perf_counter()
        try:
            r = _api_request("GET", summary_url, timeout=30)
            elapsed = round((time.perf_counter() - t0) * 1000, 1)
            body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
            out_path = OUTPUT_DIR / f"api_summary_versioned_{RUN_ID}.json"
            out_path.write_text(json.dumps({"status": r.status_code, "body": body}, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
            results["outputs"].append(str(out_path))
            if r.status_code in (200, 422):
                log("OK", "G:SUMMARY-VERSIONED", f"HTTP {r.status_code}  {elapsed}ms")
            else:
                add_problem("WARN", "G:SUMMARY-VERSIONED", f"HTTP {r.status_code}  url={summary_url}")
        except Exception as exc:
            add_problem("ERROR", "G:SUMMARY-VERSIONED", f"Request failed: {exc}")


# ──────────────────────────────────────────────────────────────────────────────
# Section F — Final report
# ──────────────────────────────────────────────────────────────────────────────

def section_f():
    log_separator("F:REPORT")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    ended_at = datetime.now(timezone.utc).isoformat()
    results["ended_at"] = ended_at

    errors = [p for p in results["problems"] if p["severity"] == "ERROR"]
    warns = [p for p in results["problems"] if p["severity"] == "WARN"]

    md_lines: list[str] = [
        f"# Compliance Gate — Retest Report",
        f"",
        f"**Run ID:** `{RUN_ID}`  ",
        f"**Started:** {results['started_at']}  ",
        f"**Ended:** {ended_at}  ",
        f"**Status:** {'✅ PASSED' if not errors else '❌ FAILED'} ({len(errors)} error(s), {len(warns)} warning(s))",
        f"",
        f"---",
        f"",
        f"## A · Files Found",
        f"",
        f"| Source | File | Size |",
        f"|--------|------|------|",
    ]

    for name, info in results["files"].items():
        sz = f"{info.get('size_bytes', 0):,} bytes"
        missing = "⚠️ MISSING" if info.get("missing") else "✅"
        md_lines.append(f"| {name} | `{Path(info['path']).name}` | {sz} {missing} |")

    md_lines += [
        f"",
        f"---",
        f"",
        f"## B · Dataframe Summary",
        f"",
        f"| Source | Rows | Cols | Read Time |",
        f"|--------|------|------|-----------|",
    ]
    for name, info in results["dataframes"].items():
        if "error" in info:
            md_lines.append(f"| {name} | ❌ parse failed | — | — |")
        else:
            md_lines.append(
                f"| {name} | {info['rows']:,} | {info['cols']} | {info.get('read_elapsed_s', '?')}s |"
            )

    # Per-source header/type detail
    for name, info in results["dataframes"].items():
        if "columns" not in info:
            continue
        md_lines += [
            f"",
            f"### {name} — Columns & Types",
            f"",
            f"| Column | Type | Nulls |",
            f"|--------|------|-------|",
        ]
        for col in info["columns"]:
            dtype = info["dtypes"].get(col, "?")
            nulls = info["null_counts"].get(col, 0)
            md_lines.append(f"| `{col}` | {dtype} | {nulls} |")

    md_lines += [
        f"",
        f"---",
        f"",
        f"## D · Validation Results",
        f"",
    ]
    for name, val in results["validation"].items():
        status = "✅ PASSED" if not val.get("missing_required") else "❌ FAILED"
        md_lines.append(f"### {name} — {status}")
        if val.get("missing_required"):
            md_lines.append(f"- **Missing required columns:** {val['missing_required']}")
        if val.get("via_alias"):
            md_lines.append(f"- **Found via alias:** {val['via_alias']}")
        if val.get("missing_optional"):
            md_lines.append(f"- ⚠️ Missing optional: {val['missing_optional']}")
        if val.get("empty_headers"):
            md_lines.append(f"- ⚠️ Empty headers: {val['empty_headers']}")
        if val.get("duplicate_headers"):
            md_lines.append(f"- ⚠️ Duplicate headers: {val['duplicate_headers']}")
        md_lines.append("")

    md_lines += [
        f"---",
        f"",
        f"## E · API Endpoint Tests",
        f"",
        f"| Endpoint | URL | HTTP | Latency |",
        f"|----------|-----|------|---------|",
    ]
    if not results["endpoints"]:
        md_lines.append("| — | API not reachable or tests skipped | — | — |")
    else:
        for key, ep in results["endpoints"].items():
            status = ep.get("status", "?")
            icon = "✅" if status == 200 else "⚠️"
            md_lines.append(
                f"| {key} | `{ep['url']}` | {icon} {status} | {ep.get('elapsed_ms', '?')}ms |"
            )

    md_lines += [
        f"",
        f"---",
        f"",
        f"## Problems Detected",
        f"",
    ]
    if not results["problems"]:
        md_lines.append("_No problems detected._")
    else:
        for p in results["problems"]:
            icon = "❌" if p["severity"] == "ERROR" else "⚠️"
            md_lines.append(f"- {icon} **[{p['severity']}]** `{p['source']}` — {p['msg']}")

    md_lines += [
        f"",
        f"---",
        f"",
        f"## Recommendations",
        f"",
    ]
    if not results["recommendations"]:
        md_lines.append("_No action required._")
    else:
        for r in results["recommendations"]:
            md_lines.append(f"- {r}")

    md_lines += [
        f"",
        f"---",
        f"",
        f"## Generated Outputs",
        f"",
    ]
    for out in results["outputs"]:
        md_lines.append(f"- `{out}`")

    md_lines += [
        f"",
        f"**Log file:** `{LOG_FILE}`",
        f"",
        f"---",
        f"_Generated by Compliance Gate Retest Orchestrator — run_retests.py_",
    ]

    report_path = OUTPUT_DIR / f"report_{RUN_ID}.md"
    report_path.write_text("\n".join(md_lines), encoding="utf-8")
    results["outputs"].append(str(report_path))
    log("OK", "F:REPORT", f"Report written → {report_path}")


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> int:
    log_separator("INIT")
    log("STEP", "INIT", f"Compliance Gate Retest Orchestrator — run_id={RUN_ID}")
    log("INFO", "INIT", f"Workspace: {WORKSPACE}")
    log("INFO", "INIT", f"API: {API_BASE_URL}  endpoint_tests={RUN_ENDPOINT_TESTS}")
    log("INFO", "INIT", f"Polars version: {pl.__version__}")

    # ── A: Pre-check
    files_ok = section_a()
    if not files_ok:
        log("ERROR", "INIT", "One or more CSV files are missing. Cannot continue.")
        section_f()
        return 1

    # ── B: Read
    frames = section_b()

    # ── C: Export
    section_c(frames)

    # ── D: Validate
    section_d(frames)

    # ── E: Endpoints
    section_e()

    # ── G: Dataset pipeline
    section_g()

    # ── F: Report
    section_f()

    # Exit code
    errors = [p for p in results["problems"] if p["severity"] == "ERROR"]
    if errors:
        log("ERROR", "FINAL", f"Retest run completed with {len(errors)} critical error(s). Exit code 1.")
        return 1

    log("OK", "FINAL", f"Retest run completed successfully. Exit code 0.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
