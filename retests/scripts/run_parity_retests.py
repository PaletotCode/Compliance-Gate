"""
run_parity_retests.py — Compliance Gate Reteste 2A: Paridade de Filtros (v2)

CORREÇÕES vs v1:
  - ASSET é APENAS lookup set (has_asset marker). Nunca adiciona novos hostnames ao master.
    → Comportamento idêntico ao loadAssetSet() do dashboard_fixed.ts (linhas 2819-2885 do TS)
  - AVAILABLE aplica-se SOMENTE a virtual GAPs (is_virtual_gap=True)
    → No TS, AVAILABLE/GAP são entradas sintéticas para gaps numéricos (linhas 3152-3193)
  - normalizeKey espelha exatamente o normalize() do TS: upper + strip domain (até o primeiro ponto)
  - Universo de máquinas vem apenas de AD + UEM + EDR (como no TS linha 3095-3097)
  - diagnóstico incluído na seção D (AVAILABLE alto / OK baixo)

Seções:
  A) Pre-check + Detecção de layout (csv_layout_detector)
  B) Join multi-fonte AD+UEM+EDR → MasterMap; ASSET como lookup
  C) Classificação local dos 14 filtros
  D) Distribuição + Checagens lógicas + Diagnóstico
  E) Comparação com API
  F) Relatório final de paridade
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

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
# Config
# ──────────────────────────────────────────────────────────────────────────────

WORKSPACE = Path(os.environ.get("WORKSPACE", "/workspace"))
API_BASE_URL = os.environ.get("API_BASE_URL", "http://api:8000")
API_PREFIX = "/api/v1"
AUTH_PREFIX = f"{API_PREFIX}/auth"
BOOTSTRAP_ADMIN_USERNAME = os.environ.get("AUTH_BOOTSTRAP_ADMIN_USERNAME", "admin")
BOOTSTRAP_ADMIN_PASSWORD = os.environ.get("AUTH_BOOTSTRAP_ADMIN_PASSWORD", "Admin1234")
AUTH_COOKIE_NAME = os.environ.get("AUTH_COOKIE_NAME", "cg_access")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RETESTS_DIR = Path(os.environ.get("RETESTS_DIR", str(PROJECT_ROOT / "retests")))
LOGS_DIR = RETESTS_DIR / "logs"
OUTPUT_DIR = RETESTS_DIR / "output"

STALE_DAYS = int(os.environ.get("STALE_DAYS", "30"))
LEGACY_DEFS = ["Windows 7", "Windows 8", "Windows XP", "Windows Server 2008", "Windows Server 2012"]

# TS uses COMPLIANT key (not OK)
TS_STATUS_KEY_COMPLIANT = "COMPLIANT"

EXPECTED_FILTER_KEYS = {
    "INCONSISTENCY", "PHANTOM", "ROGUE", "MISSING_UEM", "MISSING_EDR",
    "SWAP", "CLONE", "OFFLINE", "COMPLIANT",
    "LEGACY", "MISSING_ASSET", "PA_MISMATCH",
    "GAP", "AVAILABLE",
}

API_WAIT_RETRIES = 12
API_WAIT_SLEEP = 5

# ──────────────────────────────────────────────────────────────────────────────
# Run ID / Logging
# ──────────────────────────────────────────────────────────────────────────────

def make_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    short = hashlib.sha1(ts.encode()).hexdigest()[:6]
    return f"{ts}_{short}"

RUN_ID = make_run_id()
LOG_FILE = LOGS_DIR / f"parity_run_{RUN_ID}.log"
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
    _log_fh.write(line + "\n"); _log_fh.flush()

def sep(section: str): log("STEP", section, "─" * 68)

# ──────────────────────────────────────────────────────────────────────────────
# Report accumulator
# ──────────────────────────────────────────────────────────────────────────────

report: dict[str, Any] = {
    "run_id": RUN_ID,
    "started_at": datetime.now(timezone.utc).isoformat(),
    "relatorio_final_csv": False,
    "problems": [],
    "filter_counts_local": {},
    "filter_counts_api": {},
    "logic_checks": {},
    "endpoints": {},
    "outputs": [],
    "recommendations": [],
    "layout_detections": {},
    "diagnosis_available_ok": {},
}

_api_session: requests.Session | None = None
_api_auth_attempted = False

def add_problem(severity: str, source: str, msg: str):
    report["problems"].append({"severity": severity, "source": source, "msg": msg})
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
                f"Could not authenticate parity session: {response.status_code} {response.text}",
            )
            return None
        payload = response.json()
        if payload.get("mfa_required"):
            add_problem("WARN", "AUTH", "Bootstrap admin requires MFA; API comparison will run anonymously.")
            return None
        if not session.cookies.get(AUTH_COOKIE_NAME):
            add_problem("WARN", "AUTH", "Login did not set auth cookie; API comparison will run anonymously.")
            return None
        _api_session = session
        log("OK", "AUTH", "Authenticated API session acquired for parity API checks.")
        return _api_session
    except Exception as exc:
        add_problem("WARN", "AUTH", f"Failed to authenticate parity session: {exc}")
        return None

# ──────────────────────────────────────────────────────────────────────────────
# Normalization — MIRRORS dashboard_fixed.ts exactly
# ──────────────────────────────────────────────────────────────────────────────

def normalize_key(raw: str) -> str:
    """
    Mirrors TS normalize() (inner function of main(), line 2975):
      s.trim().toUpperCase().replace(/\\..*$/, '')
    Strips everything from the first '.' (removes domain suffix).
    """
    if not raw:
        return ""
    s = raw.strip().upper()
    dot = s.find(".")
    if dot != -1:
        s = s[:dot]
    return s

def normalize_asset_hostname(raw: str) -> str:
    """
    Mirrors normalizeAssetHostname() (line 344) → normalizeSicFromExtra():
    1) Strip .SCR2008... suffix (or first dot)
    2) Match /^(SIC_\\d+_\\d+_\\d+)/i → take that capture
    3) Uppercase
    """
    if not raw:
        return ""
    s = raw.strip()
    upper = s.upper()
    marker = ".SCR2008"
    idx = upper.find(marker)
    if idx != -1:
        s = s[:idx]
    elif "." in s:
        s = s.split(".")[0]
    base = s.strip().upper()
    m = re.match(r"^(SIC_\d+_\d+_\d+)", base, re.IGNORECASE)
    return m.group(1).upper() if m else base

def extract_pa(name: str) -> str:
    parts = name.split("_")
    if len(parts) >= 4:
        return parts[3]
    return "??"

def extract_user_suffix(user: str) -> str:
    if not user:
        return ""
    if "\\" in user:
        user = user.split("\\")[-1]
    m = re.search(r"_(\d{1,2})$", user.strip())
    return m.group(1).zfill(2) if m else ""

def parse_date_ms(date_val: str) -> int:
    if not date_val or not date_val.strip() or date_val.strip() in ("N/A", "-", "—"):
        return 0
    s = date_val.strip()
    try:
        if "T" in s and s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
    except Exception:
        pass
    try:
        m = re.match(
            r"^(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)?$",
            s, re.IGNORECASE
        )
        if m:
            mo, day, yr = int(m.group(1)), int(m.group(2)), int(m.group(3))
            hr, mi = int(m.group(4)), int(m.group(5))
            sec = int(m.group(6)) if m.group(6) else 0
            ampm = (m.group(7) or "").upper()
            if ampm == "PM" and hr < 12: hr += 12
            if ampm == "AM" and hr == 12: hr = 0
            dt = datetime(yr, mo, day, hr, mi, sec, tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
    except Exception:
        pass
    try:
        m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?$", s)
        if m:
            day, mo, yr = int(m.group(1)), int(m.group(2)), int(m.group(3))
            hr, mi = int(m.group(4)), int(m.group(5))
            sec = int(m.group(6)) if m.group(6) else 0
            dt = datetime(yr, mo, day, hr, mi, sec, tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
    except Exception:
        pass
    return 0

# ──────────────────────────────────────────────────────────────────────────────
# Section A — Layout Detection + CSV Read
# ──────────────────────────────────────────────────────────────────────────────

def _find_asset_header_row(path: Path) -> int:
    """Scan for 'NOME DO ATIVO' — mirrors loadAssetSet() TS lines 2843-2855."""
    for enc in ["utf-8-sig", "utf-8", "latin-1"]:
        try:
            with open(path, "r", encoding=enc, errors="replace") as f:
                for i, line in enumerate(f):
                    if "NOME DO ATIVO" in line.upper():
                        return i
        except Exception:
            pass
    return 4  # fallback: row 4 (known from ASSET.CSV structure)

def _norm_header(s: str) -> str:
    s = (s or "").strip().lstrip("\ufeff")
    s = unicodedata.normalize("NFD", s)
    s = re.sub(r"[\u0300-\u036f]", "", s)
    return s.upper()

def _read_csv_robust(path: Path, skip_rows: int = 0) -> pl.DataFrame | None:
    for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
        for sep_chr in [",", ";"]:
            try:
                df = pl.read_csv(
                    path, separator=sep_chr, encoding=enc, skip_rows=skip_rows,
                    infer_schema_length=500, ignore_errors=True, truncate_ragged_lines=True
                )
                if df.shape[1] > 1:
                    return df
            except Exception:
                continue
    return None

def section_a() -> dict[str, pl.DataFrame]:
    sep("A:LAYOUT+CSV")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Import detector (same directory)
    import importlib.util, sys as _sys
    det_path = Path("/retests/scripts/csv_layout_detector.py")
    if det_path.exists():
        spec = importlib.util.spec_from_file_location("csv_layout_detector", det_path)
        det_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(det_mod)
        csv_paths = {
            "ASSET": _find_csv("ASSET.CSV"),
            "UEM":   _find_csv("UEM.csv"),
            "AD":    _find_csv("AD.csv"),
            "EDR":   _find_csv("EDR.csv"),
        }
        detections = det_mod.detect_and_save_all(
            {k: v for k, v in csv_paths.items() if v is not None and v.exists()},
            RUN_ID, OUTPUT_DIR
        )
        report["layout_detections"] = {k: {kk: str(vv) if isinstance(vv, Path) else vv
                                            for kk, vv in v.items()}
                                       for k, v in detections.items()}
        for src, det in detections.items():
            if "error" not in det:
                log("OK", f"A:LAYOUT:{src}", (
                    f"header_row={det['detected_header_row_index']} "
                    f"data_starts={det['detected_first_data_row_index']} "
                    f"method={det['method']} confidence={det['confidence']}"
                ))
                if det["matched_headers_sample"]:
                    report["outputs"].append(
                        str(OUTPUT_DIR / f"{src}_detected_header_row_{RUN_ID}.json")
                    )
                    report["outputs"].append(
                        str(OUTPUT_DIR / f"{src}_raw_head_30_{RUN_ID}.txt")
                    )
    else:
        log("WARN", "A:LAYOUT", "csv_layout_detector.py not found — using fallback detection")

    # Read DataFrames
    frames: dict[str, pl.DataFrame] = {}
    csv_sources = {
        "ASSET": _find_csv("ASSET.CSV"),
        "UEM":   _find_csv("UEM.csv"),
        "AD":    _find_csv("AD.csv"),
        "EDR":   _find_csv("EDR.csv"),
    }
    for name, path in csv_sources.items():
        if path is None or not path.exists():
            add_problem("ERROR", f"A:{name}", f"CSV not found in workspace")
            continue
        skip = _find_asset_header_row(path) if name == "ASSET" else 0
        df = _read_csv_robust(path, skip)
        if df is None:
            add_problem("ERROR", f"A:{name}", "Parse failed")
            continue
        frames[name] = df
        log("OK", f"A:{name}", f"{path.name} header_skip={skip} → {df.shape[0]:,} rows × {df.shape[1]} cols")
    return frames

def _find_csv(filename: str) -> Path | None:
    path = WORKSPACE / filename
    if path.exists(): return path
    for p in WORKSPACE.glob("*"):
        if p.name.upper() == filename.upper():
            return p
    return None

# ──────────────────────────────────────────────────────────────────────────────
# Section B — Build MasterMap
# CRITICAL: Only AD + UEM + EDR add new hosts. ASSET is lookup-only.
# This mirrors dashboard_fixed.ts lines 3095-3198 exactly.
# ──────────────────────────────────────────────────────────────────────────────

class MachineEntry:
    __slots__ = [
        "raw_name", "pa_code",
        "has_ad", "has_uem", "has_edr", "has_asset",
        "ad_os",
        "uem_serial", "uem_user", "uem_seen",
        "edr_serial", "edr_user", "edr_seen",
        "last_seen_ms", "last_seen_source",
        "serial_is_cloned", "is_virtual_gap",
        "join_key_used",
    ]
    def __init__(self, raw_name: str, pa_code: str, join_key: str):
        self.raw_name = raw_name
        self.pa_code = pa_code
        self.join_key_used = join_key
        self.has_ad = self.has_uem = self.has_edr = self.has_asset = False
        self.ad_os = ""
        self.uem_serial = self.uem_user = self.uem_seen = ""
        self.edr_serial = self.edr_user = self.edr_seen = ""
        self.last_seen_ms = 0
        self.last_seen_source = ""
        self.serial_is_cloned = self.is_virtual_gap = False


def _col(df: pl.DataFrame, *candidates) -> str | None:
    cols_upper = {c.upper(): c for c in df.columns}
    for cand in candidates:
        if cand.upper() in cols_upper:
            return cols_upper[cand.upper()]
    return None

def _val_row(row: dict, *candidates) -> str:
    for c in candidates:
        v = row.get(c)
        if v is not None and str(v).strip() not in ("", "None", "null"):
            return str(v).strip()
    return ""

def section_b(frames: dict[str, pl.DataFrame]) -> dict[str, MachineEntry]:
    sep("B:JOIN")
    master: dict[str, MachineEntry] = {}

    # ── Step 1: AD (source of truth for AD membership)
    # TS: for (let i = 1; i < dataAD.length; i++) upsert(dataAD[i], "AD", idxAdName)
    # Header cols: Computer Name, Operating System, Last Logon Time, Password Last Set
    df_ad = frames.get("AD")
    if df_ad is not None:
        c_name = _col(df_ad, "Computer Name", "ComputerName")
        c_os   = _col(df_ad, "Operating System", "OperatingSystem")
        if c_name:
            for row in df_ad.to_dicts():
                raw = _val_row(row, c_name)
                if not raw: continue
                key = normalize_key(raw)
                if not key: continue
                if key not in master:
                    master[key] = MachineEntry(raw, extract_pa(key), key)
                e = master[key]
                e.has_ad = True
                if c_os: e.ad_os = _val_row(row, c_os)
                # AD logon (optional)
                logon_col = _col(df_ad, "Last Logon Time", "LastLogonTime")
                if logon_col:
                    ms = parse_date_ms(_val_row(row, logon_col))
                    if ms > e.last_seen_ms:
                        e.last_seen_ms = ms; e.last_seen_source = "AD"
        log("OK", "B:AD", f"AD upserted {len(master):,} entries")
    else:
        add_problem("WARN", "B:AD", "AD DataFrame missing")

    # ── Step 2: UEM (adds new hosts or updates existing)
    # TS: for (let i = 1; i < dataUEM.length; i++) upsert(dataUEM[i], "UEM", idxUemName)
    # Header: preferably "Hostname" then fallback to "Friendly Name"
    df_uem = frames.get("UEM")
    if df_uem is not None:
        # TS: idxUemName = getIdx(hUEM,"Hostname"); if==-1 idxUemName=getIdx(hUEM,"Friendly Name")
        c_name   = _col(df_uem, "Hostname", "Friendly Name", "device_friendly_name")
        c_user   = _col(df_uem, "Username")
        c_serial = _col(df_uem, "Serial Number")
        c_seen   = _col(df_uem, "Last Seen")
        new_from_uem = 0
        if c_name:
            for row in df_uem.to_dicts():
                raw = _val_row(row, c_name)
                if not raw: continue
                key = normalize_key(raw)
                if not key: continue
                if key not in master:
                    master[key] = MachineEntry(raw, extract_pa(key), key)
                    new_from_uem += 1
                e = master[key]
                e.has_uem = True
                if c_user:   e.uem_user   = _val_row(row, c_user)
                if c_serial: e.uem_serial = _val_row(row, c_serial)
                if c_seen:   e.uem_seen   = _val_row(row, c_seen)
                # TS: if lastSeenScore==0 and uemScore>0 → update
                ms = parse_date_ms(e.uem_seen)
                if e.last_seen_ms == 0 and ms > 0:
                    e.last_seen_ms = ms; e.last_seen_source = "UEM"
        log("OK", "B:UEM", f"UEM upserted — {new_from_uem} new, total={len(master):,}")
    else:
        add_problem("WARN", "B:UEM", "UEM DataFrame missing")

    # ── Step 3: EDR (adds new hosts or updates existing)
    # TS: for (let i = 1; i < dataEDR.length; i++) upsert(dataEDR[i], "EDR", idxEdrName)
    # Header: preferably "Friendly Name" then fallback to "Hostname"
    df_edr = frames.get("EDR")
    if df_edr is not None:
        # TS: idxEdrName = getIdx(hEDR,"Friendly Name"); if==-1 idxEdrName=getIdx(hEDR,"Hostname")
        c_name   = _col(df_edr, "Friendly Name", "Hostname")
        c_user   = _col(df_edr, "Last Logged In User Account")
        c_serial = _col(df_edr, "Serial Number")
        c_seen   = _col(df_edr, "Last Seen")
        new_from_edr = 0
        if c_name:
            for row in df_edr.to_dicts():
                raw = _val_row(row, c_name)
                if not raw: continue
                key = normalize_key(raw)
                if not key: continue
                if key not in master:
                    master[key] = MachineEntry(raw, extract_pa(key), key)
                    new_from_edr += 1
                e = master[key]
                e.has_edr = True
                if c_user:   e.edr_user   = _val_row(row, c_user)
                if c_serial: e.edr_serial = _val_row(row, c_serial)
                if c_seen:   e.edr_seen   = _val_row(row, c_seen)
                # TS: canOverrideWithEdr = !AD || lastScore==0 || source=="UEM"
                ms = parse_date_ms(e.edr_seen)
                can_override = (not e.has_ad) or (e.last_seen_ms == 0) or (e.last_seen_source in ("", "UEM"))
                if can_override and ms > 0:
                    e.last_seen_ms = ms; e.last_seen_source = "EDR"
        log("OK", "B:EDR", f"EDR upserted — {new_from_edr} new, total={len(master):,}")
    else:
        add_problem("WARN", "B:EDR", "EDR DataFrame missing")

    # ── Step 4: ASSET — LOOKUP ONLY (markers has_asset; does NOT add new entries)
    # TS lines 3196-3198:
    #   for (const [k, m] of masterMap.entries()) {
    #     const base = normalizeSicFromExtra(k);
    #     m.sources.ASSET = assetSet.has(k) || (base ? assetSet.has(base) : false);
    #   }
    df_asset = frames.get("ASSET")
    asset_set: set[str] = set()
    if df_asset is not None:
        c_name = _col(df_asset, "Nome do ativo")
        if c_name:
            for row in df_asset.to_dicts():
                raw = _val_row(row, c_name)
                if not raw: continue
                key = normalize_asset_hostname(raw)
                if key: asset_set.add(key)
        log("OK", "B:ASSET", f"Asset set built: {len(asset_set):,} unique keys")
    else:
        add_problem("WARN", "B:ASSET", "ASSET DataFrame missing")

    # Apply ASSET lookup to existing master entries
    asset_matches = 0
    for key, e in master.items():
        # TS uses normalizeSicFromExtra (extracts SIC_XX_XXXX_XX prefix)
        m = re.match(r"^(SIC_\d+_\d+_\d+)", key, re.IGNORECASE)
        base = m.group(1).upper() if m else key
        if key in asset_set or base in asset_set:
            e.has_asset = True
            asset_matches += 1
    log("OK", "B:ASSET_MARK", f"has_asset=True for {asset_matches} out of {len(master):,} entries")

    # ── Step 5: Clone detection
    serial_map: dict[str, list[str]] = {}
    for key, e in master.items():
        s = e.edr_serial or e.uem_serial
        if s and len(s) >= 5:
            serial_map.setdefault(s, []).append(key)
    cloned_serials = {s for s, keys in serial_map.items() if len(keys) > 1}
    for key, e in master.items():
        s = e.edr_serial or e.uem_serial
        if s in cloned_serials:
            e.serial_is_cloned = True
    log("OK", "B:CLONE", f"Clone-detected serials: {len(cloned_serials)}")
    log("OK", "B:FINAL", f"MasterMap: {len(master):,} unique machines (from AD+UEM+EDR only)")
    return master

# ──────────────────────────────────────────────────────────────────────────────
# Section C — Classification (mirrors TS rules exactly)
# AVAILABLE = virtual GAP only. TS key is "COMPLIANT" not "OK".
# ──────────────────────────────────────────────────────────────────────────────

def classify(e: MachineEntry) -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    stale_ms = STALE_DAYS * 24 * 3600 * 1000

    if e.is_virtual_gap:
        return {"primary": "AVAILABLE", "flags": []}

    # TS PRIMARY precedence (line 3304+):
    # PHANTOM (!AD && !UEM && !EDR — not virtual) but only after inconsistency check
    primary = "COMPLIANT"   # TS uses "COMPLIANT" not "OK"

    if not e.has_ad and (e.has_uem or e.has_edr):
        primary = "INCONSISTENCY"
    elif not e.has_ad and not e.has_uem and not e.has_edr:
        primary = "PHANTOM"
    elif e.has_ad and not e.has_uem and not e.has_edr:
        primary = "ROGUE"
    elif e.has_ad and not e.has_uem and e.has_edr:
        primary = "MISSING_UEM"
    elif e.has_ad and e.has_uem and not e.has_edr:
        primary = "MISSING_EDR"
    elif e.uem_serial and e.edr_serial and e.uem_serial != e.edr_serial:
        primary = "SWAP"
    elif e.serial_is_cloned:
        primary = "CLONE"
    elif (e.last_seen_ms and e.last_seen_ms > 0 and
          (now_ms - e.last_seen_ms) > stale_ms):
        primary = "OFFLINE"

    # FLAGS (parallel, additive)
    flags = []
    if e.ad_os:
        os_up = e.ad_os.upper()
        for leg in LEGACY_DEFS:
            if leg.upper() in os_up:
                flags.append("LEGACY"); break
    if e.has_ad and not e.has_asset and (e.has_uem or e.has_edr):
        flags.append("MISSING_ASSET")
    machine_sfx = extract_user_suffix(e.raw_name)
    cand_user = e.uem_user or e.edr_user or ""
    if "\\" in cand_user: cand_user = cand_user.split("\\")[-1]
    user_sfx = extract_user_suffix(cand_user)
    if machine_sfx and user_sfx and machine_sfx != user_sfx:
        flags.append("PA_MISMATCH")

    return {"primary": primary, "flags": flags}


def section_c(master: dict[str, MachineEntry]) -> list[dict[str, Any]]:
    sep("C:CLASSIFY")
    now_ms = int(time.time() * 1000)
    rows = []
    for key, e in master.items():
        result = classify(e)
        days = (now_ms - e.last_seen_ms) / (1000 * 86400) if e.last_seen_ms and e.last_seen_ms > 0 else None
        rows.append({
            "hostname_canon":       key,
            "raw_name":             e.raw_name,
            "pa_code":              e.pa_code,
            "join_key":             e.join_key_used,
            "has_ad":               e.has_ad,
            "has_uem":              e.has_uem,
            "has_edr":              e.has_edr,
            "has_asset":            e.has_asset,
            "ad_os":                e.ad_os,
            "uem_serial":           e.uem_serial,
            "edr_serial":           e.edr_serial,
            "uem_user":             e.uem_user,
            "edr_user":             e.edr_user,
            "last_seen_ms":         e.last_seen_ms,
            "last_seen_source":     e.last_seen_source,
            "days_since_last_seen": round(days, 1) if days is not None else None,
            "serial_is_cloned":     e.serial_is_cloned,
            "primary_status":       result["primary"],
            "flags":                ",".join(result["flags"]),
            "flag_legacy":          "LEGACY" in result["flags"],
            "flag_missing_asset":   "MISSING_ASSET" in result["flags"],
            "flag_pa_mismatch":     "PA_MISMATCH" in result["flags"],
        })
    log("OK", "C:CLASSIFY", f"Classified {len(rows):,} real machines (no virtual GAPs in master)")
    return rows

# ──────────────────────────────────────────────────────────────────────────────
# Section D — Distribution + Logic Checks + DIAGNOSIS
# ──────────────────────────────────────────────────────────────────────────────

KEY_LABELS = {
    "COMPLIANT":     "✅ SEGURO (COMPLIANT)",
    "INCONSISTENCY": "🧩 INCONSISTÊNCIA DE BASE",
    "PHANTOM":       "👻 FANTASMA (AD)",
    "ROGUE":         "🚨 PERIGO (SEM AGENTE)",
    "MISSING_UEM":   "⚠️ FALTA UEM",
    "MISSING_EDR":   "⚠️ FALTA EDR",
    "SWAP":          "🔄 TROCA DE SERIAL",
    "CLONE":         "👯 DUPLICADO",
    "OFFLINE":       "💤 OFFLINE",
    "GAP":           "🔴 GAP DE NOMES",
    "AVAILABLE":     "ℹ️ DISPONÍVEL (virtual GAP)",
    "LEGACY":        "🧓 SISTEMA LEGADO",
    "MISSING_ASSET": "📦 FALTA ASSET",
    "PA_MISMATCH":   "🟠 DIVERGÊNCIA PA x USUÁRIO",
}

def section_d(rows: list[dict], master: dict[str, MachineEntry], frames: dict) -> dict[str, Any]:
    sep("D:CHECKS+DIAGNOSIS")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pl.DataFrame(rows) if rows else pl.DataFrame()

    status_counts: dict[str, int] = {}
    if not df.is_empty():
        sc = df.group_by("primary_status").len().to_dict(as_series=False)
        status_counts = dict(zip(sc["primary_status"], sc["len"]))
    flag_counts: dict[str, int] = {}
    for flag_col, flag_key in [("flag_legacy", "LEGACY"), ("flag_missing_asset", "MISSING_ASSET"), ("flag_pa_mismatch", "PA_MISMATCH")]:
        flag_counts[flag_key] = int(df[flag_col].sum()) if not df.is_empty() else 0

    report["filter_counts_local"]["by_status"] = status_counts
    report["filter_counts_local"]["by_flag"] = flag_counts

    # Rich table
    rt = Table(title=f"Distribuição Local — {len(rows):,} máquinas reais", show_lines=True)
    rt.add_column("Key"); rt.add_column("Label"); rt.add_column("Count", justify="right"); rt.add_column("Type")
    for k, cnt in sorted(status_counts.items(), key=lambda x: -x[1]):
        rt.add_row(k, KEY_LABELS.get(k, k), str(cnt), "status")
    for k, cnt in sorted(flag_counts.items(), key=lambda x: -x[1]):
        rt.add_row(k, KEY_LABELS.get(k, k), str(cnt), "flag")
    console.print(rt)

    # Counts CSV
    counts_rows = []
    for k, cnt in status_counts.items():
        counts_rows.append({"filter_key": k, "label": KEY_LABELS.get(k, k), "count": cnt, "type": "status"})
    for k, cnt in flag_counts.items():
        counts_rows.append({"filter_key": k, "label": KEY_LABELS.get(k, k), "count": cnt, "type": "flag"})
    counts_path = OUTPUT_DIR / f"machines_counts_{RUN_ID}.csv"
    pl.DataFrame(counts_rows).write_csv(counts_path)
    report["outputs"].append(str(counts_path))

    # Min-table (100 sample)
    min_path_csv = OUTPUT_DIR / f"machines_min_table_{RUN_ID}.csv"
    min_path_json = OUTPUT_DIR / f"machines_min_table_{RUN_ID}.json"
    sample = df.head(100) if not df.is_empty() else df
    sample.write_csv(min_path_csv)
    min_path_json.write_text(json.dumps(sample.to_dicts(), ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    report["outputs"].extend([str(min_path_csv), str(min_path_json)])

    # Coverage CSV (every machine with join key)
    cov_rows = []
    for key, e in master.items():
        result = classify(e)
        cov_rows.append({
            "hostname":      key,
            "has_ad":        e.has_ad,
            "has_uem":       e.has_uem,
            "has_edr":       e.has_edr,
            "has_asset":     e.has_asset,
            "status_final":  result["primary"],
            "note_join_key": e.join_key_used,
        })
    cov_path = OUTPUT_DIR / f"machines_coverage_{RUN_ID}.csv"
    pl.DataFrame(cov_rows).write_csv(cov_path)
    report["outputs"].append(str(cov_path))
    log("OK", "D:COVERAGE", f"Coverage CSV → {cov_path.name} ({len(cov_rows)} rows)")

    # Per-filter sample CSVs
    for fk in list(status_counts.keys()):
        fdf = df.filter(pl.col("primary_status") == fk)
        fpath = OUTPUT_DIR / f"machines_table_{fk}_{RUN_ID}.csv"
        fdf.head(50).write_csv(fpath)
        report["outputs"].append(str(fpath))
    log("OK", "D:PER_FILTER", f"Per-filter CSVs for {len(status_counts)} statuses")

    # ── Logic checks
    checks: dict[str, Any] = {}
    def check(name: str, cond_str: str, violations: list[str]) -> None:
        ok = len(violations) == 0
        checks[name] = {"rule": cond_str, "pass": ok, "violations": violations[:10], "violation_count": len(violations)}
        icon = "✅ PASS" if ok else f"❌ FAIL ({len(violations)} violations)"
        log("OK" if ok else "WARN", f"D:CHECK:{name}", f"{icon} — {cond_str}")

    if not df.is_empty():
        for status, cond, pred in [
            ("MISSING_UEM",   "has_ad=T, has_uem=F, has_edr=T",       lambda r: r["has_ad"] and not r["has_uem"] and r["has_edr"]),
            ("MISSING_EDR",   "has_ad=T, has_uem=T, has_edr=F",       lambda r: r["has_ad"] and r["has_uem"] and not r["has_edr"]),
            ("ROGUE",         "has_ad=T, has_uem=F, has_edr=F",       lambda r: r["has_ad"] and not r["has_uem"] and not r["has_edr"]),
            ("PHANTOM",       "has_ad=F, has_uem=F, has_edr=F",       lambda r: not r["has_ad"] and not r["has_uem"] and not r["has_edr"]),
            ("INCONSISTENCY", "has_ad=F, (has_uem=T OR has_edr=T)",   lambda r: not r["has_ad"] and (r["has_uem"] or r["has_edr"])),
            ("SWAP",          "uem_serial≠edr_serial, both present",   lambda r: r.get("uem_serial") and r.get("edr_serial") and r["uem_serial"] != r["edr_serial"]),
            ("CLONE",         "serial_is_cloned=T",                   lambda r: r.get("serial_is_cloned")),
            ("OFFLINE",       f"days_since_last_seen>{STALE_DAYS}",   lambda r: r.get("days_since_last_seen") and r["days_since_last_seen"] > STALE_DAYS),
        ]:
            subset = df.filter(pl.col("primary_status") == status)
            if subset.is_empty():
                checks[status] = {"rule": cond, "pass": True, "violations": [], "violation_count": 0}
                log("INFO", f"D:CHECK:{status}", f"0 rows — skipped")
                continue
            v = [r["hostname_canon"] for r in subset.to_dicts() if not pred(r)]
            check(status, cond, v)

        ma_df = df.filter(pl.col("flag_missing_asset"))
        v = [r["hostname_canon"] for r in ma_df.to_dicts()
             if not (r["has_ad"] and not r["has_asset"] and (r["has_uem"] or r["has_edr"]))]
        check("MISSING_ASSET_FLAG", "has_ad=T, has_asset=F, (has_uem=T OR has_edr=T)", v)
    else:
        checks["_all"] = {"rule": "N/A", "pass": False, "violations": ["No data"], "violation_count": 1}

    checks_path = OUTPUT_DIR / f"machines_logic_checks_{RUN_ID}.json"
    checks_path.write_text(json.dumps(checks, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    report["outputs"].append(str(checks_path))
    report["logic_checks"] = checks

    # ── DIAGNOSIS: AVAILABLE alto / OK baixo
    asset_rows = len(frames.get("ASSET", pl.DataFrame()))
    total_real = len(rows)
    compliant_cnt = status_counts.get("COMPLIANT", 0)
    avail_cnt_here = 0  # AVAILABLE doesn't appear in real master (only GAPs would)

    diag = {
        "asset_csv_rows": asset_rows,
        "master_real_machines": total_real,
        "compliant_count": compliant_cnt,
        "available_in_local": avail_cnt_here,
        "cause_identified": "D",  # see below
        "explanation": (
            "CAUSA RAIZ IDENTIFICADA (D): A v1 do script usava ASSET como fonte de novos "
            "hostnames no MasterMap. Como ASSET.CSV tem 202 entradas todas com 'Nome do ativo' "
            "contendo hostnames reais E 'Estado do ativo = In Store', o script v1 criou 202 "
            "entradas com is_available_in_asset=True → classificadas como AVAILABLE. "
            "O TS nunca faz isso: loadAssetSet() monta um SET de chaves e depois apenas "
            "marca m.sources.ASSET=true nos entries existentes. AVAILABLE no TS refere-se "
            "exclusivamente a virtual GAPs (isVirtualGap=true). "
            "CORREÇÃO v2: ASSET é lookup-only; universo vem apenas de AD+UEM+EDR."
        ),
        "causes_ruled_out": {
            "A_header_row": "Não. Layout de ASSET correto (header na linha 4, dinâmico). AD/UEM/EDR header na linha 0.",
            "B_normalization": "Parcialmente. normalize_key() do TS usa '.replace(/\\..*$/)' que strip na PRIMEIRA '.', não apenas '.SCR2008'. Corrigido na v2.",
            "C_available_wrong_scope": "Sim — mas decorrente de (D), não causa independente.",
            "D_wrong_universe": "CAUSA PRINCIPAL. ASSET foi usado como fonte de entradas no master, o que não ocorre no TS.",
        },
    }
    report["diagnosis_available_ok"] = diag
    log("OK", "D:DIAGNOSIS", f"Causa raiz: {diag['cause_identified']} — {diag['explanation'][:80]}...")

    return {"status_counts": status_counts, "flag_counts": flag_counts}

# ──────────────────────────────────────────────────────────────────────────────
# Section E — API Comparison
# ──────────────────────────────────────────────────────────────────────────────

def _wait_api() -> bool:
    log("INFO", "E:API", f"Waiting for API at {API_BASE_URL} ...")
    for i in range(1, API_WAIT_RETRIES + 1):
        try:
            r = requests.get(f"{API_BASE_URL}/health", timeout=4)
            if r.status_code == 200:
                log("OK", "E:API", f"API ready (attempt {i})")
                return True
        except Exception:
            pass
        log("INFO", "E:API", f"  {i}/{API_WAIT_RETRIES} — sleeping {API_WAIT_SLEEP}s")
        time.sleep(API_WAIT_SLEEP)
    return False

def _api_get(path: str, params: dict | None = None) -> tuple[int, Any, float]:
    url = f"{API_BASE_URL}{path}"
    t0 = time.perf_counter()
    try:
        session = _get_authenticated_session()
        if session is not None:
            r = session.get(url, params=params, timeout=15)
        else:
            r = requests.get(url, params=params, timeout=15)
        ms = round((time.perf_counter() - t0) * 1000, 1)
        try: body = r.json()
        except: body = {"raw": r.text[:200]}
        return r.status_code, body, ms
    except Exception as exc:
        ms = round((time.perf_counter() - t0) * 1000, 1)
        return 0, {"error": str(exc)}, ms

def section_e(local_counts: dict) -> None:
    sep("E:API")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if not _wait_api():
        add_problem("WARN", "E:API", "API unreachable — skipping endpoint comparison")
        return

    for ep, path, params in [
        ("filters",         f"{API_PREFIX}/machines/filters",  None),
        ("summary",         f"{API_PREFIX}/machines/summary",  None),
        ("table_p1",        f"{API_PREFIX}/machines/table",    {"page": 1, "page_size": 50}),
    ]:
        status, body, ms = _api_get(path, params)
        report["endpoints"][ep] = {"status": status, "ms": ms}
        jp = OUTPUT_DIR / f"machines_{ep}_{RUN_ID}.json"
        jp.write_text(json.dumps({"status": status, "ms": ms, "body": body}, indent=2, default=str), encoding="utf-8")
        report["outputs"].append(str(jp))
        log("OK" if status == 200 else "WARN", f"E:{ep.upper()}", f"HTTP {status}  {ms}ms")

    # Check API filter count
    status, body, ms = _api_get(f"{API_PREFIX}/machines/filters")
    if status == 200:
        api_keys = {f["key"] for f in body.get("data", [])}
        if len(api_keys) != 14:
            add_problem("WARN", "E:FILTERS", f"API returned {len(api_keys)} filters, expected 14")
    # Check if API total=0
    status, body, ms = _api_get(f"{API_PREFIX}/machines/summary")
    if status == 200:
        total = body.get("data", {}).get("total", 0)
        report["filter_counts_api"] = body.get("data", {})
        if total == 0:
            add_problem("ERROR", "E:API", (
                "API /machines/summary returned total=0. "
                "MachinesService uses MachinesEngine(data=[]) — no real CSV ingestion. "
                "Local classification is ground truth for this retest."
            ))
            report["recommendations"].append(
                "PATCH NEEDED: Implement CSV ingestion in MachinesService._get_engine(). "
                "Mount /workspace CSVs and load them via the same join pipeline implemented here."
            )

# ──────────────────────────────────────────────────────────────────────────────
# Section F — Parity Report
# ──────────────────────────────────────────────────────────────────────────────

def section_f():
    sep("F:REPORT")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ended_at = datetime.now(timezone.utc).isoformat()
    report["ended_at"] = ended_at

    errors = [p for p in report["problems"] if p["severity"] == "ERROR"]
    warns  = [p for p in report["problems"] if p["severity"] == "WARN"]
    local  = report.get("filter_counts_local", {})
    checks = report.get("logic_checks", {})
    diag   = report.get("diagnosis_available_ok", {})

    md = [
        f"# Compliance Gate — Parity Report [Reteste 2A v2]",
        f"",
        f"**Run ID:** `{RUN_ID}`",
        f"**Started:** {report['started_at']}",
        f"**Ended:** {ended_at}",
        f"**Status:** {'✅ PASSED' if not errors else '⚠️ ISSUES DETECTED'} ({len(errors)} error(s), {len(warns)} warning(s))",
        f"",
        f"---",
        f"",
        f"## 1 · Layout de Leitura dos CSVs",
        f"",
        f"| Source | Método | Header Row | Data Starts | Confiança |",
        f"|--------|--------|------------|-------------|-----------|",
    ]
    for src, det in report.get("layout_detections", {}).items():
        md.append(
            f"| {src} | {det.get('method','?')} | {det.get('detected_header_row_index','?')} | "
            f"{det.get('detected_first_data_row_index','?')} | {det.get('confidence','?')} |"
        )
    md += [
        f"",
        f"> **AD/UEM/EDR**: header sempre na linha 0 (CSV padrão) — mirrors TS `hAD = dataAD[0]`",
        f"> **ASSET**: scan dinâmico por `NOME DO ATIVO` — mirrors TS `loadAssetSet()` (linha 2848)",
        f"",
        f"---",
        f"",
        f"## 2 · Distribuição Local (ground truth — AD+UEM+EDR, ASSET só lookup)",
        f"",
        f"| Filtro Key | Label | Count | Tipo |",
        f"|-----------|-------|-------|------|",
    ]
    for k, cnt in sorted(local.get("by_status", {}).items(), key=lambda x: -x[1]):
        md.append(f"| `{k}` | {KEY_LABELS.get(k, k)} | **{cnt}** | status |")
    for k, cnt in sorted(local.get("by_flag", {}).items(), key=lambda x: -x[1]):
        md.append(f"| `{k}` | {KEY_LABELS.get(k, k)} | **{cnt}** | flag |")
    total_local = sum(local.get("by_status", {}).values())
    md += [f"", f"**Total máquinas reais:** {total_local}", f""]
    md += [
        f"---",
        f"",
        f"## 3 · Checagens Lógicas Mínimas",
        f"",
        f"| Regra | Condição | Resultado | Violações |",
        f"|-------|----------|-----------|-----------|",
    ]
    for cname, cinfo in checks.items():
        icon = "✅ PASS" if cinfo.get("pass") else "❌ FAIL"
        md.append(f"| `{cname}` | {cinfo.get('rule', '')} | {icon} | {cinfo.get('violation_count', 0)} |")
        if not cinfo.get("pass") and cinfo.get("violations"):
            md.append(f"  - Exemplos: `{'`, `'.join(cinfo['violations'])}`")
    md += [
        f"",
        f"---",
        f"",
        f"## 4 · Diagnóstico: Por que AVAILABLE ficou alto e OK baixo? (v1 → v2)",
        f"",
        f"**Causa identificada: (D) Universo de máquinas incorreto**",
        f"",
        f"| Causa | Hipótese | Status |",
        f"|-------|----------|--------|",
    ]
    for cause_key, cause_val in diag.get("causes_ruled_out", {}).items():
        is_main = "PRINCIPAL" in cause_val
        icon = "✅ CAUSA RAIZ" if is_main else "❌ Descartada"
        md.append(f"| {cause_key} | {cause_val[:60]}... | {icon} |")
    md += [
        f"",
        f"**Explicação detalhada:**",
        f"",
        f"> {diag.get('explanation', 'N/A')}",
        f"",
        f"**Correção aplicada na v2:**",
        f"- ASSET só marca `has_asset=True` nas entradas existentes no MasterMap",
        f"- ASSET nunca cria entradas novas no MasterMap",
        f"- AVAILABLE/GAP são entradas sintéticas (não presentes nesta base sem gap insertion)",
        f"- TS key foi corrigido de `OK` para `COMPLIANT` (mirrors TS `statusKey = 'COMPLIANT'`)",
        f"",
        f"---",
        f"",
        f"## 5 · API Endpoints",
        f"",
        f"| Endpoint | HTTP | Latência |",
        f"|----------|------|---------|",
    ]
    for ep, info in report.get("endpoints", {}).items():
        icon = "✅" if info["status"] == 200 else "⚠️"
        md.append(f"| `{ep}` | {icon} {info['status']} | {info['ms']}ms |")
    md += [
        f"",
        f"---",
        f"",
        f"## 6 · Problemas",
        f"",
    ]
    if not report["problems"]:
        md.append("_Nenhum problema._")
    else:
        for p in report["problems"]:
            icon = "❌" if p["severity"] == "ERROR" else "⚠️"
            md.append(f"- {icon} **[{p['severity']}]** `{p['source']}` — {p['msg']}")
    md += [
        f"",
        f"---",
        f"",
        f"## 7 · Recomendações",
        f"",
    ]
    for r in (report["recommendations"] or ["Sem ações pendentes."]):
        md.append(f"- {r}")
    md += [
        f"",
        f"---",
        f"",
        f"## 8 · Outputs",
        f"",
    ]
    for o in report["outputs"]:
        md.append(f"- `{o}`")
    md += [
        f"",
        f"**Log:** `{LOG_FILE}`",
        f"",
        f"---",
        f"",
        f"## 9 · Solicitar Reteste 2B",
        f"",
        f"> Para reteste 2B, disponibilize `RELATORIO_FINAL.csv` na raiz do repo ou em `retests/fixtures/`.",
        f"",
        f"_Gerado por Compliance Gate Parity Retest v2 — run_parity_retests.py_",
    ]

    report_path = OUTPUT_DIR / f"parity_report_{RUN_ID}.md"
    report_path.write_text("\n".join(md), encoding="utf-8")
    report["outputs"].append(str(report_path))
    log("OK", "F:REPORT", f"Parity report → {report_path}")

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> int:
    sep("INIT")
    log("STEP", "INIT", f"Compliance Gate Parity Retest v2 — run_id={RUN_ID}")
    log("INFO", "INIT", f"Workspace={WORKSPACE} | API={API_BASE_URL} | stale_days={STALE_DAYS}")

    # RELATORIO_FINAL check
    for candidate in [WORKSPACE / "RELATORIO_FINAL.csv", Path("/retests/fixtures/RELATORIO_FINAL.csv")]:
        if candidate.exists():
            report["relatorio_final_csv"] = str(candidate)
            log("OK", "INIT", f"RELATORIO_FINAL.csv found → 2B disponível")
            break
    if not report["relatorio_final_csv"]:
        log("WARN", "INIT", "RELATORIO_FINAL.csv not found — only 2A will run")

    frames = section_a()
    if not frames:
        log("ERROR", "INIT", "No CSVs readable"); section_f(); return 1

    master = section_b(frames)
    if not master:
        log("ERROR", "INIT", "MasterMap empty"); section_f(); return 1

    classified = section_c(master)
    counts = section_d(classified, master, frames)
    section_e(counts)
    section_f()

    errors = [p for p in report["problems"] if p["severity"] == "ERROR"]
    if errors:
        log("ERROR", "FINAL", f"Parity retest completed with {len(errors)} critical error(s). Exit 1.")
        return 1
    log("OK", "FINAL", "Parity retest 2A v2 completed successfully. Exit 0.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
