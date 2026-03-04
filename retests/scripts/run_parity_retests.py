"""
run_parity_retests.py — Compliance Gate Reteste 2A: Paridade de Filtros

Estratégia:
  - Ingere os 4 CSVs reais (ASSET, UEM, AD, EDR) da raiz do workspace
  - Constrói MachineRecord equivalentes ao backend (join multi-fonte)
  - Aplica a MESMA lógica de classificação dos 14 filtros do backend (replicada localmente)
  - Compara contagens com o que a API retorna via /machines/summary e /machines/table
  - Gera evidências (CSVs, JSONs, report.md) por run_id

Seções:
  A) Pre-check + Leitura dos CSVs
  B) Join multi-fonte (AD + UEM + EDR + ASSET) → MasterMap
  C) Classificação local dos 14 filtros
  D) Distribuição + Validação de consistência lógica
  E) Comparação com API (endpoints reais)
  F) Relatório final de paridade

Exit codes:
  0 → ok (warnings permitidos)
  1 → erro crítico
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import polars as pl
import requests
from rich.console import Console
from rich.table import Table

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────

WORKSPACE = Path(os.environ.get("WORKSPACE", "/workspace"))
API_BASE_URL = os.environ.get("API_BASE_URL", "http://api:8000")
API_PREFIX = "/api/v1"

RETESTS_DIR = Path("/retests")
LOGS_DIR = RETESTS_DIR / "logs"
OUTPUT_DIR = RETESTS_DIR / "output"
SCRIPTS_DIR = Path("/retests/scripts")

STALE_DAYS = int(os.environ.get("STALE_DAYS", "30"))
LEGACY_DEFS = ["Windows 7", "Windows 8", "Windows XP", "Windows Server 2008", "Windows Server 2012"]

CSV_FILES = {
    "ASSET": WORKSPACE / "ASSET.CSV",
    "UEM":   WORKSPACE / "UEM.csv",
    "AD":    WORKSPACE / "AD.csv",
    "EDR":   WORKSPACE / "EDR.csv",
}

# 14 expected filter keys
EXPECTED_FILTER_KEYS = {
    "INCONSISTENCY", "PHANTOM", "ROGUE", "MISSING_UEM", "MISSING_EDR",
    "SWAP", "CLONE", "OFFLINE", "OK",
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
# Accumulators
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
}

def add_problem(severity: str, source: str, msg: str):
    report["problems"].append({"severity": severity, "source": source, "msg": msg})
    log(severity, source, msg)

# ──────────────────────────────────────────────────────────────────────────────
# Normalization helpers (mirrors dashboard_fixed.ts)
# ──────────────────────────────────────────────────────────────────────────────

def _strip_ext(s: str) -> str:
    """Remove .scr2008... or any domain suffix."""
    if not s:
        return ""
    upper = s.upper()
    idx = upper.find(".SCR2008")
    if idx != -1:
        s = s[:idx]
    elif "." in s:
        s = s.split(".")[0]
    return s

def normalize_key(raw: str) -> str:
    """Canonical hostname key: uppercase, strip domain suffix."""
    if not raw:
        return ""
    s = raw.strip()
    s = _strip_ext(s)
    return s.upper().replace(" ", "")

def extract_pa(name: str) -> str:
    """Extract PA code from hostname SIC_XX_PACODE_NN → PACODE."""
    parts = name.split("_")
    if len(parts) >= 4:
        return parts[3]
    return "??"

def extract_user_suffix(user: str) -> str:
    """Extract trailing _NN from a username, zero-padded to 2."""
    if not user:
        return ""
    if "\\" in user:
        user = user.split("\\")[-1]
    m = re.search(r"_(\d{1,2})$", user.strip())
    return m.group(1).zfill(2) if m else ""

def parse_date_ms(date_val: str) -> int:
    """Parse a date string to milliseconds since epoch. Returns 0 on failure."""
    if not date_val or not date_val.strip() or date_val.strip() in ("N/A", "-"):
        return 0
    s = date_val.strip()
    # ISO 8601 (EDR)
    try:
        if "T" in s and s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
    except Exception:
        pass
    # US format: MM/DD/YYYY HH:MM[:SS] [AM|PM]
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
    # PT format: DD/MM/YYYY HH:MM[:SS]
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
# Section A — Read CSVs
# ──────────────────────────────────────────────────────────────────────────────

def _find_asset_header_row(path: Path) -> int:
    for enc in ["utf-8-sig", "utf-8", "latin-1"]:
        try:
            with open(path, "r", encoding=enc, errors="replace") as f:
                for i, line in enumerate(f):
                    if "NOME DO ATIVO" in line.upper():
                        return i
        except Exception:
            pass
    return 4

def _read_csv_polars(path: Path, skip_rows: int = 0) -> pl.DataFrame | None:
    for enc in ["utf-8-sig", "utf-8", "latin-1"]:
        for sep in [",", ";"]:
            try:
                df = pl.read_csv(
                    path, separator=sep, encoding=enc, skip_rows=skip_rows,
                    infer_schema_length=500, ignore_errors=True, truncate_ragged_lines=True
                )
                if df.shape[1] > 1:
                    return df
            except Exception:
                continue
    return None

def section_a() -> dict[str, pl.DataFrame]:
    sep("A:PRE-CHECK")
    frames = {}
    missing = []
    for name, path in CSV_FILES.items():
        if not path.exists():
            # try case-insensitive
            found = next((p for p in path.parent.glob("*") if p.name.upper() == path.name.upper()), None)
            if found:
                CSV_FILES[name] = found
                path = found
            else:
                add_problem("ERROR", f"A:{name}", f"File not found: {path}")
                missing.append(name)
                continue

        skip = _find_asset_header_row(path) if name == "ASSET" else 0
        df = _read_csv_polars(path, skip)
        if df is None:
            add_problem("ERROR", f"A:{name}", "CSV parse failed")
            missing.append(name)
            continue
        frames[name] = df
        log("OK", f"A:{name}", f"{path.name} → {df.shape[0]:,} rows × {df.shape[1]} cols")
    if missing:
        log("ERROR", "A:PRE-CHECK", f"Missing critical files: {missing}")
    return frames

# ──────────────────────────────────────────────────────────────────────────────
# Section B — Build MasterMap (join AD + UEM + EDR + ASSET)
# ──────────────────────────────────────────────────────────────────────────────

class MachineEntry:
    __slots__ = [
        "raw_name", "pa_code",
        "has_ad", "has_uem", "has_edr", "has_asset",
        "ad_os",
        "uem_serial", "uem_user", "uem_seen", "uem_dm_seen", "uem_extra_user",
        "edr_serial", "edr_user", "edr_seen", "edr_login",
        "last_seen_ms", "last_seen_source",
        "serial_is_cloned", "is_virtual_gap", "is_available_in_asset",
    ]
    def __init__(self, raw_name: str, pa_code: str):
        self.raw_name = raw_name
        self.pa_code = pa_code
        self.has_ad = self.has_uem = self.has_edr = self.has_asset = False
        self.ad_os = ""
        self.uem_serial = self.uem_user = self.uem_seen = self.uem_dm_seen = self.uem_extra_user = ""
        self.edr_serial = self.edr_user = self.edr_seen = self.edr_login = ""
        self.last_seen_ms = 0
        self.last_seen_source = ""
        self.serial_is_cloned = self.is_virtual_gap = self.is_available_in_asset = False


def _col(df: pl.DataFrame, *candidates) -> str | None:
    """Return the first matching column name (case-insensitive)."""
    cols_upper = {c.upper(): c for c in df.columns}
    for cand in candidates:
        if cand.upper() in cols_upper:
            return cols_upper[cand.upper()]
    return None

def _val(row: dict, *candidates) -> str:
    for c in candidates:
        v = row.get(c)
        if v is not None and str(v).strip() not in ("", "None", "null"):
            return str(v).strip()
    return ""

def section_b(frames: dict[str, pl.DataFrame]) -> dict[str, MachineEntry]:
    sep("B:JOIN")
    master: dict[str, MachineEntry] = {}

    # ── AD
    df_ad = frames.get("AD")
    if df_ad is not None:
        c_name = _col(df_ad, "Computer Name", "ComputerName", "hostname")
        c_os   = _col(df_ad, "Operating System", "OperatingSystem")
        if c_name:
            for row in df_ad.to_dicts():
                raw = _val(row, c_name)
                if not raw:
                    continue
                key = normalize_key(raw)
                if not key:
                    continue
                if key not in master:
                    master[key] = MachineEntry(raw, extract_pa(key))
                e = master[key]
                e.has_ad = True
                if c_os:
                    e.ad_os = _val(row, c_os)
                # last_seen from AD logon (optional col)
                logon_col = _col(df_ad, "Last Logon Time", "LastLogonTime")
                if logon_col:
                    ms = parse_date_ms(_val(row, logon_col))
                    if ms > e.last_seen_ms:
                        e.last_seen_ms = ms
                        e.last_seen_source = "AD"
        log("OK", "B:AD", f"Inserted/updated {len(master):,} entries from AD")
    else:
        add_problem("WARN", "B:AD", "AD DataFrame missing — has_ad will be False for all")

    # ── UEM
    df_uem = frames.get("UEM")
    if df_uem is not None:
        c_name   = _col(df_uem, "Friendly Name", "Hostname", "device_friendly_name")
        c_user   = _col(df_uem, "Username")
        c_serial = _col(df_uem, "Serial Number")
        c_seen   = _col(df_uem, "Last Seen")
        c_dmseen = _col(df_uem, "DM Last Seen")
        if c_name:
            new_from_uem = 0
            for row in df_uem.to_dicts():
                raw = _val(row, c_name)
                if not raw:
                    continue
                key = normalize_key(raw)
                if not key:
                    continue
                if key not in master:
                    master[key] = MachineEntry(raw, extract_pa(key))
                    new_from_uem += 1
                e = master[key]
                e.has_uem = True
                if c_user:   e.uem_user   = _val(row, c_user)
                if c_serial: e.uem_serial = _val(row, c_serial)
                if c_seen:   e.uem_seen   = _val(row, c_seen)
                if c_dmseen: e.uem_dm_seen = _val(row, c_dmseen)
                # last_seen override (UEM is secondary to EDR)
                ms = parse_date_ms(e.uem_seen)
                if e.last_seen_source == "" and ms > 0:
                    e.last_seen_ms = ms
                    e.last_seen_source = "UEM"
            log("OK", "B:UEM", f"Processed UEM — {new_from_uem} new entries, total {len(master):,}")
        else:
            add_problem("WARN", "B:UEM", "Friendly Name column not found in UEM")
    else:
        add_problem("WARN", "B:UEM", "UEM DataFrame missing")

    # ── EDR
    df_edr = frames.get("EDR")
    if df_edr is not None:
        c_name   = _col(df_edr, "Hostname", "Friendly Name")
        c_user   = _col(df_edr, "Last Logged In User Account")
        c_serial = _col(df_edr, "Serial Number")
        c_seen   = _col(df_edr, "Last Seen")
        c_login  = _col(df_edr, "Last User Account Login")
        if c_name:
            new_from_edr = 0
            for row in df_edr.to_dicts():
                raw = _val(row, c_name)
                if not raw:
                    continue
                key = normalize_key(raw)
                if not key:
                    continue
                if key not in master:
                    master[key] = MachineEntry(raw, extract_pa(key))
                    new_from_edr += 1
                e = master[key]
                e.has_edr = True
                if c_user:   e.edr_user   = _val(row, c_user)
                if c_serial: e.edr_serial = _val(row, c_serial)
                if c_seen:   e.edr_seen   = _val(row, c_seen)
                if c_login:  e.edr_login  = _val(row, c_login)
                # EDR overrides previous last_seen (except AD takes precedence in TS logic;
                # but TS actually prefers EDR when AD is absent or UEM was source)
                ms = parse_date_ms(e.edr_seen)
                can_override = (not e.has_ad) or (e.last_seen_ms == 0) or (e.last_seen_source in ("", "UEM"))
                if can_override and ms > 0:
                    e.last_seen_ms = ms
                    e.last_seen_source = "EDR"
            log("OK", "B:EDR", f"Processed EDR — {new_from_edr} new entries, total {len(master):,}")
        else:
            add_problem("WARN", "B:EDR", "Hostname column not found in EDR")
    else:
        add_problem("WARN", "B:EDR", "EDR DataFrame missing")

    # ── ASSET (mark has_asset)
    df_asset = frames.get("ASSET")
    if df_asset is not None:
        c_name = _col(df_asset, "Nome do ativo")
        c_state = _col(df_asset, "Estado do ativo")
        if c_name:
            asset_hits = 0
            for row in df_asset.to_dicts():
                raw = _val(row, c_name)
                if not raw:
                    continue
                key = normalize_key(raw)
                if not key:
                    continue
                if key not in master:
                    master[key] = MachineEntry(raw, extract_pa(key))
                e = master[key]
                e.has_asset = True
                # "In Store" / "Disponível" → is_available_in_asset
                if c_state:
                    state = _val(row, c_state).upper()
                    if state in ("IN STORE", "DISPONÍVEL", "DISPONIVEL", "AVAILABLE"):
                        e.is_available_in_asset = True
                asset_hits += 1
            log("OK", "B:ASSET", f"Marked {asset_hits} entries from ASSET, total master={len(master):,}")
        else:
            add_problem("WARN", "B:ASSET", "'Nome do ativo' column not found in ASSET")
    else:
        add_problem("WARN", "B:ASSET", "ASSET DataFrame missing")

    # ── CLONE detection (same edr_serial → multiple hostnames)
    serial_map: dict[str, list[str]] = {}
    for key, e in master.items():
        if e.edr_serial:
            serial_map.setdefault(e.edr_serial, []).append(key)
    cloned_serials = {s for s, keys in serial_map.items() if len(keys) > 1}
    for key, e in master.items():
        if e.edr_serial in cloned_serials:
            e.serial_is_cloned = True

    log("OK", "B:JOIN", f"MasterMap total: {len(master):,} unique hostnames | Cloned serials: {len(cloned_serials)}")
    return master

# ──────────────────────────────────────────────────────────────────────────────
# Section C — Local classification (mirrors backend rules exactly)
# ──────────────────────────────────────────────────────────────────────────────

def classify(e: MachineEntry) -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    stale_ms = STALE_DAYS * 24 * 3600 * 1000

    # ── Special / bypass
    if e.is_virtual_gap:
        return {"primary": "GAP", "flags": []}
    if e.is_available_in_asset:
        return {"primary": "AVAILABLE", "flags": []}

    # ── Primary (first-match wins, same precedence as orchestrator)
    primary = "OK"
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

    # ── Flags (parallel, additive)
    flags = []
    # LEGACY: ad_os contains legacy string
    if e.ad_os:
        os_up = e.ad_os.upper()
        for leg in LEGACY_DEFS:
            if leg.upper() in os_up:
                flags.append("LEGACY")
                break
    # MISSING_ASSET: AD present, not in asset, UEM or EDR present
    if e.has_ad and not e.has_asset and (e.has_uem or e.has_edr):
        flags.append("MISSING_ASSET")
    # PA_MISMATCH: suffix of hostname != suffix of user
    machine_sfx = extract_user_suffix(e.raw_name)
    candidate_user = e.uem_extra_user or e.uem_user or e.edr_user or ""
    if "\\" in candidate_user:
        candidate_user = candidate_user.split("\\")[-1]
    user_sfx = extract_user_suffix(candidate_user)
    if machine_sfx and user_sfx and machine_sfx != user_sfx:
        flags.append("PA_MISMATCH")

    return {"primary": primary, "flags": flags}


def section_c(master: dict[str, MachineEntry]) -> list[dict[str, Any]]:
    sep("C:CLASSIFY")
    rows = []
    now_ms = int(time.time() * 1000)

    for key, e in master.items():
        result = classify(e)
        days = (now_ms - e.last_seen_ms) / (1000 * 86400) if e.last_seen_ms and e.last_seen_ms > 0 else None
        rows.append({
            "hostname_canon":       key,
            "raw_name":             e.raw_name,
            "pa_code":              e.pa_code,
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
            "is_available":         e.is_available_in_asset,
            "primary_status":       result["primary"],
            "flags":                ",".join(result["flags"]),
            "flag_legacy":          "LEGACY" in result["flags"],
            "flag_missing_asset":   "MISSING_ASSET" in result["flags"],
            "flag_pa_mismatch":     "PA_MISMATCH" in result["flags"],
        })

    log("OK", "C:CLASSIFY", f"Classified {len(rows):,} records")
    return rows

# ──────────────────────────────────────────────────────────────────────────────
# Section D — Distribution + Logic Checks
# ──────────────────────────────────────────────────────────────────────────────

KEY_LABELS = {
    "OK":            "✅ SEGURO (OK)",
    "INCONSISTENCY": "🧩 INCONSISTÊNCIA DE BASE",
    "PHANTOM":       "👻 FANTASMA (AD)",
    "ROGUE":         "🚨 PERIGO (SEM AGENTE)",
    "MISSING_UEM":   "⚠️ FALTA UEM",
    "MISSING_EDR":   "⚠️ FALTA EDR",
    "SWAP":          "🔄 TROCA DE SERIAL",
    "CLONE":         "👯 DUPLICADO",
    "OFFLINE":       "💤 OFFLINE",
    "GAP":           "🔴 GAP DE NOMES",
    "AVAILABLE":     "ℹ️ DISPONÍVEL",
    "LEGACY":        "🧓 SISTEMA LEGADO",
    "MISSING_ASSET": "📦 FALTA ASSET",
    "PA_MISMATCH":   "🟠 DIVERGÊNCIA PA x USUÁRIO",
}

def section_d(rows: list[dict]) -> dict[str, Any]:
    sep("D:CHECKS")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pl.DataFrame(rows)

    # Counts by primary status
    status_counts: dict[str, int] = {}
    if not df.is_empty():
        sc = df.group_by("primary_status").len().to_dict(as_series=False)
        status_counts = dict(zip(sc["primary_status"], sc["len"]))

    # Flag counts
    flag_counts: dict[str, int] = {}
    for flag_col, flag_key in [("flag_legacy", "LEGACY"), ("flag_missing_asset", "MISSING_ASSET"), ("flag_pa_mismatch", "PA_MISMATCH")]:
        flag_counts[flag_key] = int(df[flag_col].sum()) if not df.is_empty() else 0

    report["filter_counts_local"]["by_status"] = status_counts
    report["filter_counts_local"]["by_flag"] = flag_counts

    # Rich table
    rt = Table(title=f"Distribuição Local — {len(rows):,} máquinas", show_lines=True)
    rt.add_column("Filtro (Key)"); rt.add_column("Label"); rt.add_column("Count", justify="right"); rt.add_column("Type")
    for k, cnt in sorted(status_counts.items(), key=lambda x: -x[1]):
        rt.add_row(k, KEY_LABELS.get(k, k), str(cnt), "status")
    for k, cnt in sorted(flag_counts.items(), key=lambda x: -x[1]):
        rt.add_row(k, KEY_LABELS.get(k, k), str(cnt), "flag")
    console.print(rt)

    # Save counts CSV
    counts_rows = []
    for k, cnt in status_counts.items():
        counts_rows.append({"filter_key": k, "label": KEY_LABELS.get(k, k), "count": cnt, "type": "status"})
    for k, cnt in flag_counts.items():
        counts_rows.append({"filter_key": k, "label": KEY_LABELS.get(k, k), "count": cnt, "type": "flag"})
    counts_path = OUTPUT_DIR / f"machines_counts_{RUN_ID}.csv"
    pl.DataFrame(counts_rows).write_csv(counts_path)
    report["outputs"].append(str(counts_path))
    log("OK", "D:COUNTS", f"Counts saved → {counts_path.name}")

    # Save full min-table
    min_path_csv = OUTPUT_DIR / f"machines_min_table_{RUN_ID}.csv"
    min_path_json = OUTPUT_DIR / f"machines_min_table_{RUN_ID}.json"
    sample = df.head(100) if not df.is_empty() else df
    sample.write_csv(min_path_csv)
    sample.to_dicts()  # validate
    min_path_json.write_text(
        json.dumps(sample.to_dicts(), ensure_ascii=False, indent=2, default=str),
        encoding="utf-8"
    )
    report["outputs"].extend([str(min_path_csv), str(min_path_json)])
    log("OK", "D:MIN_TABLE", f"Min-table (100 sample) → {min_path_csv.name}")

    # Save per-filter sample CSVs
    all_filter_keys = list(status_counts.keys()) + list(flag_counts.keys())
    for fk in all_filter_keys:
        if fk in status_counts:
            fdf = df.filter(pl.col("primary_status") == fk)
        else:
            # It's a flag
            col_map = {"LEGACY": "flag_legacy", "MISSING_ASSET": "flag_missing_asset", "PA_MISMATCH": "flag_pa_mismatch"}
            col = col_map.get(fk)
            fdf = df.filter(pl.col(col) == True) if col else df.head(0)
        fpath = OUTPUT_DIR / f"machines_table_{fk}_{RUN_ID}.csv"
        fdf.head(50).write_csv(fpath)
        report["outputs"].append(str(fpath))

    log("OK", "D:PER_FILTER", f"Per-filter CSVs written for {len(all_filter_keys)} filters")

    # Logic checks
    checks: dict[str, Any] = {}

    def check(name: str, expected_cond: str, violations: list[str]) -> dict:
        ok = len(violations) == 0
        result = {
            "rule": expected_cond,
            "pass": ok,
            "violations": violations[:10],  # cap at 10
            "violation_count": len(violations),
        }
        checks[name] = result
        icon = "✅ PASS" if ok else f"❌ FAIL ({len(violations)} violations)"
        log("OK" if ok else "WARN", f"D:CHECK:{name}", f"{icon} — {expected_cond}")
        return result

    if not df.is_empty():
        # MISSING_UEM: has_ad AND NOT has_uem AND has_edr (matches ROGUE+MISSING_UEM combined; MISSING_UEM specifically is has_ad AND !uem AND has_edr)
        uem_df = df.filter(pl.col("primary_status") == "MISSING_UEM")
        v = [r["hostname_canon"] for r in uem_df.to_dicts() if not (r["has_ad"] and not r["has_uem"] and r["has_edr"])]
        check("MISSING_UEM", "has_ad=T, has_uem=F, has_edr=T", v)

        # MISSING_EDR: has_ad AND has_uem AND NOT has_edr
        edr_df = df.filter(pl.col("primary_status") == "MISSING_EDR")
        v = [r["hostname_canon"] for r in edr_df.to_dicts() if not (r["has_ad"] and r["has_uem"] and not r["has_edr"])]
        check("MISSING_EDR", "has_ad=T, has_uem=T, has_edr=F", v)

        # ROGUE: has_ad AND NOT has_uem AND NOT has_edr
        rogue_df = df.filter(pl.col("primary_status") == "ROGUE")
        v = [r["hostname_canon"] for r in rogue_df.to_dicts() if not (r["has_ad"] and not r["has_uem"] and not r["has_edr"])]
        check("ROGUE", "has_ad=T, has_uem=F, has_edr=F", v)

        # PHANTOM: NOT has_ad AND NOT has_uem AND NOT has_edr
        phantom_df = df.filter(pl.col("primary_status") == "PHANTOM")
        v = [r["hostname_canon"] for r in phantom_df.to_dicts() if r["has_ad"] or r["has_uem"] or r["has_edr"]]
        check("PHANTOM", "has_ad=F, has_uem=F, has_edr=F", v)

        # INCONSISTENCY: NOT has_ad AND (has_uem OR has_edr)
        inc_df = df.filter(pl.col("primary_status") == "INCONSISTENCY")
        v = [r["hostname_canon"] for r in inc_df.to_dicts() if r["has_ad"] or (not r["has_uem"] and not r["has_edr"])]
        check("INCONSISTENCY", "has_ad=F, (has_uem=T OR has_edr=T)", v)

        # SWAP: uem_serial != edr_serial AND both present
        swap_df = df.filter(pl.col("primary_status") == "SWAP")
        v = [r["hostname_canon"] for r in swap_df.to_dicts()
             if not (r.get("uem_serial") and r.get("edr_serial") and r["uem_serial"] != r["edr_serial"])]
        check("SWAP", "uem_serial≠edr_serial, both present", v)

        # CLONE: serial_is_cloned = True
        clone_df = df.filter(pl.col("primary_status") == "CLONE")
        v = [r["hostname_canon"] for r in clone_df.to_dicts() if not r.get("serial_is_cloned")]
        check("CLONE", "serial_is_cloned=T", v)

        # MISSING_ASSET flag: has_ad AND NOT has_asset AND (has_uem OR has_edr)
        ma_df = df.filter(pl.col("flag_missing_asset") == True)
        v = [r["hostname_canon"] for r in ma_df.to_dicts()
             if not (r["has_ad"] and not r["has_asset"] and (r["has_uem"] or r["has_edr"]))]
        check("MISSING_ASSET_FLAG", "has_ad=T, has_asset=F, (has_uem=T OR has_edr=T)", v)

        # OFFLINE: days_since > STALE_DAYS
        off_df = df.filter(pl.col("primary_status") == "OFFLINE")
        v = [r["hostname_canon"] for r in off_df.to_dicts()
             if not (r.get("days_since_last_seen") and r["days_since_last_seen"] > STALE_DAYS)]
        check("OFFLINE", f"days_since_last_seen>{STALE_DAYS}", v)

    else:
        checks["_all"] = {"rule": "N/A", "pass": False, "violations": ["No data to check"], "violation_count": 1}
        add_problem("WARN", "D:CHECKS", "No records to run logic checks on")

    checks_path = OUTPUT_DIR / f"machines_logic_checks_{RUN_ID}.json"
    checks_path.write_text(json.dumps(checks, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    report["outputs"].append(str(checks_path))
    report["logic_checks"] = checks
    log("OK", "D:CHECKS", f"Logic checks saved → {checks_path.name}")

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
        r = requests.get(url, params=params, timeout=15)
        ms = round((time.perf_counter() - t0) * 1000, 1)
        try:
            body = r.json()
        except Exception:
            body = {"raw": r.text[:200]}
        return r.status_code, body, ms
    except Exception as exc:
        ms = round((time.perf_counter() - t0) * 1000, 1)
        return 0, {"error": str(exc)}, ms

def section_e(local_counts: dict) -> None:
    sep("E:API")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not _wait_api():
        add_problem("WARN", "E:API", "API unreachable — skipping endpoint comparison")
        report["recommendations"].append(
            "API was unreachable. Ensure the api container is healthy and retry."
        )
        return

    # /machines/filters
    status, body, ms = _api_get(f"{API_PREFIX}/machines/filters")
    report["endpoints"]["filters"] = {"status": status, "ms": ms}
    fpath = OUTPUT_DIR / f"machines_filters_{RUN_ID}.json"
    fpath.write_text(json.dumps({"status": status, "ms": ms, "body": body}, indent=2, default=str), encoding="utf-8")
    report["outputs"].append(str(fpath))

    if status == 200:
        api_filters = body.get("data", [])
        api_keys = {f["key"] for f in api_filters}
        missing_keys = EXPECTED_FILTER_KEYS - api_keys
        extra_keys = api_keys - EXPECTED_FILTER_KEYS
        log("OK", "E:FILTERS", f"API returned {len(api_keys)} filter keys")
        if missing_keys:
            add_problem("WARN", "E:FILTERS", f"Missing expected filter keys in API: {missing_keys}")
        if extra_keys:
            log("INFO", "E:FILTERS", f"Extra keys in API (not in expected set): {extra_keys}")
    else:
        add_problem("ERROR", "E:FILTERS", f"GET /machines/filters → HTTP {status}")

    # /machines/summary
    status, body, ms = _api_get(f"{API_PREFIX}/machines/summary")
    report["endpoints"]["summary"] = {"status": status, "ms": ms}
    spath = OUTPUT_DIR / f"machines_summary_{RUN_ID}.json"
    spath.write_text(json.dumps({"status": status, "ms": ms, "body": body}, indent=2, default=str), encoding="utf-8")
    report["outputs"].append(str(spath))

    api_summary = {}
    if status == 200:
        d = body.get("data", {})
        api_by_status = d.get("by_status", {})
        api_by_flag = d.get("by_flag", {})
        api_summary = {"by_status": api_by_status, "by_flag": api_by_flag}
        report["filter_counts_api"] = api_summary
        log("OK", "E:SUMMARY", f"API summary: total={d.get('total', '?')}, statuses={list(api_by_status.keys())}")

        # Critical finding: if API total = 0, the engine has no data (mock)
        if d.get("total", 0) == 0:
            add_problem("ERROR", "E:API", (
                "API /machines/summary returned total=0. "
                "The MachinesService is using MachinesEngine(data=[]) — "
                "no real CSV ingestion in the API layer yet. "
                "Local classification results are the ground truth for this retest."
            ))
            report["recommendations"].append(
                "CRITICAL: API engine has no data (data=[]). "
                "To achieve true end-to-end paridade, implement CSV ingestion in MachinesService._get_engine(). "
                "The local classification in this retest script is the reference implementation."
            )
    else:
        add_problem("ERROR", "E:SUMMARY", f"GET /machines/summary → HTTP {status}")

    # /machines/table (page 1)
    status, body, ms = _api_get(f"{API_PREFIX}/machines/table", params={"page": 1, "page_size": 50})
    report["endpoints"]["table"] = {"status": status, "ms": ms}
    tpath = OUTPUT_DIR / f"machines_table_api_{RUN_ID}.json"
    tpath.write_text(json.dumps({"status": status, "ms": ms, "body": body}, indent=2, default=str), encoding="utf-8")
    report["outputs"].append(str(tpath))
    if status == 200:
        items = body.get("data", {}).get("items", [])
        log("OK", "E:TABLE", f"GET /machines/table → {len(items)} items returned (API engine may be empty)")
    else:
        add_problem("ERROR", "E:TABLE", f"GET /machines/table → HTTP {status}")

    # Per-filter table calls (using statuses= query param; will be empty if API has no data)
    local_status_keys = local_counts.get("status_counts", {}).keys()
    for fk in local_status_keys:
        status, body, ms = _api_get(f"{API_PREFIX}/machines/table", params={"statuses": fk, "page": 1, "page_size": 50})
        jp = OUTPUT_DIR / f"machines_table_{fk}_{RUN_ID}.json"
        jp.write_text(json.dumps({"status": status, "ms": ms, "body": body}, indent=2, default=str), encoding="utf-8")
        report["outputs"].append(str(jp))
        items_count = len(body.get("data", {}).get("items", [])) if status == 200 else 0
        level = "OK" if status == 200 else "WARN"
        log(level, f"E:TABLE:{fk}", f"HTTP {status} → {items_count} items  ({ms}ms)")

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

    local = report.get("filter_counts_local", {})
    api   = report.get("filter_counts_api", {})
    checks = report.get("logic_checks", {})

    md = [
        f"# Compliance Gate — Parity Report [Reteste 2A]",
        f"",
        f"**Run ID:** `{RUN_ID}`  ",
        f"**Started:** {report['started_at']}  ",
        f"**Ended:** {ended_at}  ",
        f"**Status:** {'✅ PASSED' if not errors else '⚠️ ISSUES DETECTED'} ({len(errors)} error(s), {len(warns)} warning(s))",
        f"**RELATORIO_FINAL.csv:** {'✅ Available (2B ran)' if report['relatorio_final_csv'] else '❌ Not found — 2B skipped'}",
        f"",
        f"---",
        f"",
        f"## 1 · Distribuição Local (ground truth desde os CSVs reais)",
        f"",
        f"| Filtro Key | Label | Count | Tipo |",
        f"|-----------|-------|-------|------|",
    ]
    for k, cnt in sorted(local.get("by_status", {}).items(), key=lambda x: -x[1]):
        md.append(f"| `{k}` | {KEY_LABELS.get(k, k)} | **{cnt}** | status |")
    for k, cnt in sorted(local.get("by_flag", {}).items(), key=lambda x: -x[1]):
        md.append(f"| `{k}` | {KEY_LABELS.get(k, k)} | **{cnt}** | flag |")

    total_local = sum(local.get("by_status", {}).values())
    md += [f"", f"**Total de máquinas únicas:** {total_local}", f""]

    md += [
        f"---",
        f"",
        f"## 2 · API Endpoint Summary",
        f"",
        f"| Endpoint | HTTP | Latência |",
        f"|----------|------|---------|",
    ]
    for ep, info in report.get("endpoints", {}).items():
        icon = "✅" if info["status"] == 200 else "⚠️"
        md.append(f"| `{ep}` | {icon} {info['status']} | {info['ms']}ms |")

    api_total = sum(api.get("by_status", {}).values()) if api else 0
    md += [f"", f"**API total machines:** {api_total}"]
    if api_total == 0:
        md.append(f"  > ⚠️ **API retornou 0 registros** — MachinesService usa `data=[]`. A paridade 1:1 está pendente da implementação de CSV ingestion no backend.")

    md += [
        f"",
        f"---",
        f"",
        f"## 3 · Comparação Local vs API",
        f"",
        f"| Filtro | Local Count | API Count | Match? |",
        f"|--------|------------|-----------|--------|",
    ]
    all_keys = set(local.get("by_status", {}).keys()) | set(api.get("by_status", {}).keys())
    for k in sorted(all_keys):
        lc = local.get("by_status", {}).get(k, 0)
        ac = api.get("by_status", {}).get(k, 0)
        match = "✅" if lc == ac else "❌" if api_total > 0 else "⏭️ API sem dados"
        md.append(f"| `{k}` | {lc} | {ac} | {match} |")

    md += [
        f"",
        f"---",
        f"",
        f"## 4 · Checagens Lógicas Mínimas (2A)",
        f"",
        f"| Regra | Condição | Resultado | Violações |",
        f"|-------|----------|-----------|-----------|",
    ]
    for cname, cinfo in checks.items():
        icon = "✅ PASS" if cinfo.get("pass") else "❌ FAIL"
        vcount = cinfo.get("violation_count", 0)
        md.append(f"| `{cname}` | {cinfo.get('rule', '')} | {icon} | {vcount} |")
        if not cinfo.get("pass") and cinfo.get("violations"):
            md.append(f"  - Violations (até 10): `{'`, `'.join(cinfo['violations'])}`")

    md += [
        f"",
        f"---",
        f"",
        f"## 5 · Problemas Detectados",
        f"",
    ]
    if not report["problems"]:
        md.append("_Nenhum problema detectado._")
    else:
        for p in report["problems"]:
            icon = "❌" if p["severity"] == "ERROR" else "⚠️"
            md.append(f"- {icon} **[{p['severity']}]** `{p['source']}` — {p['msg']}")

    md += [
        f"",
        f"---",
        f"",
        f"## 6 · Recomendações",
        f"",
    ]
    if not report["recommendations"]:
        md.append("_Sem ações pendentes._")
    else:
        for r in report["recommendations"]:
            md.append(f"- {r}")

    md += [
        f"",
        f"---",
        f"",
        f"## 7 · Outputs Gerados",
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
        f"## 8 · Solicitar Reteste 2B",
        f"",
        f"> Para executar o **Reteste 2B (paridade direta 1:1 com Excel)**, disponibilize o arquivo `RELATORIO_FINAL.csv`",
        f"> exportado do Excel OfficeScript (mesmo dia e mesmos CSVs de entrada) em:",
        f"> - Raiz do repositório: `RELATORIO_FINAL.csv`",
        f"> - ou em: `retests/fixtures/RELATORIO_FINAL.csv`",
        f"",
        f"_Gerado por Compliance Gate Parity Retest — run_parity_retests.py_",
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
    log("STEP", "INIT", f"Compliance Gate Parity Retest — run_id={RUN_ID}")
    log("INFO", "INIT", f"Workspace={WORKSPACE} | API={API_BASE_URL} | stale_days={STALE_DAYS}")

    # Check RELATORIO_FINAL
    for candidate in [WORKSPACE / "RELATORIO_FINAL.csv", RETESTS_DIR / "fixtures" / "RELATORIO_FINAL.csv"]:
        if candidate.exists():
            report["relatorio_final_csv"] = str(candidate)
            log("OK", "INIT", f"RELATORIO_FINAL.csv found → {candidate} (2B will run)")
            break
    if not report["relatorio_final_csv"]:
        log("WARN", "INIT", "RELATORIO_FINAL.csv not found — only 2A will run")

    frames = section_a()
    if len([f for f in frames.values() if f is not None]) == 0:
        log("ERROR", "INIT", "No CSVs readable. Aborting.")
        section_f()
        return 1

    master = section_b(frames)
    if not master:
        log("ERROR", "INIT", "MasterMap empty after join. Aborting.")
        section_f()
        return 1

    classified = section_c(master)
    counts = section_d(classified)
    section_e(counts)
    section_f()

    errors = [p for p in report["problems"] if p["severity"] == "ERROR"]
    if errors:
        log("ERROR", "FINAL", f"Parity retest completed with {len(errors)} critical error(s). Exit 1.")
        return 1
    log("OK", "FINAL", "Parity retest 2A completed successfully. Exit 0.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
