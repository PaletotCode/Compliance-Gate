"""
master_map_builder.py — Multi-source join for the Machines domain.

Mirrors dashboard_fixed.ts main() join logic (lines 3003-3097):
  1. Upsert from AD (primary source)
  2. Upsert from UEM (adds or updates existing entries)
  3. Upsert from EDR (adds or updates existing entries)
  4. Apply ASSET lookup-only (marks has_asset, NEVER creates new entries)
  5. Detect cloned serials
  Produces List[Dict] ready for MachineRecord(**dict) → engine._ingest_and_classify()

Key normalization: strip everything from first '.' → matches TS normalize() (line 2976).
ASSET is applied after the full join, same as TS lines 3196-3198.
"""

from __future__ import annotations

import logging
import re
import time
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Optional

import polars as pl

from compliance_gate.infra.storage.csv_loader import MachinesSources

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Normalisation — mirrors TS exactly
# ──────────────────────────────────────────────────────────────────────────────

def normalize_key(raw: str) -> str:
    """
    Mirrors TS normalize() inner function (line 2975-2982):
      s.toString().trim().toUpperCase().replace(/\\..*$/, '')
    Strips everything from the first '.' onward.
    """
    if not raw:
        return ""
    s = raw.strip().upper()
    dot = s.find(".")
    return s[:dot] if dot != -1 else s


def normalize_asset_key(raw: str) -> str:
    """
    Mirrors normalizeAssetHostname() → normalizeSicFromExtra() (lines 344-359).
    Used only for building the asset_set.
    """
    if not raw:
        return ""
    s = raw.strip()
    upper = s.upper()
    idx = upper.find(".SCR2008")
    if idx != -1:
        s = s[:idx]
    elif "." in s:
        s = s.split(".")[0]
    base = s.strip().upper()
    m = re.match(r"^(SIC_\d+_\d+_\d+)", base, re.IGNORECASE)
    return m.group(1).upper() if m else base


def extract_pa(key: str) -> str:
    """Mirrors TS extractPa() (line 2995): split by '_', take index 3 if len>=4."""
    parts = key.split("_")
    return parts[3] if len(parts) >= 4 else "??"


# ──────────────────────────────────────────────────────────────────────────────
# Column resolution helpers (mirrors TS getIdx with aliases)
# ──────────────────────────────────────────────────────────────────────────────

def _col(df: pl.DataFrame, *candidates: str) -> Optional[str]:
    """Find the first matching column name (case-insensitive)."""
    upper_map = {c.upper(): c for c in df.columns}
    for cand in candidates:
        found = upper_map.get(cand.upper())
        if found is not None:
            return found
    return None


def _val(row: dict[str, Any], *candidates: str) -> str:
    """Get first non-empty string value from a row dict."""
    for c in candidates:
        v = row.get(c)
        if v is not None:
            sv = str(v).strip()
            if sv and sv not in ("None", "null", "N/A", "-", "—"):
                return sv
    return ""


# ──────────────────────────────────────────────────────────────────────────────
# Machine entry accumulator
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class _Entry:
    hostname: str        # canonical normalized key
    raw_name: str        # original name from first source
    pa_code: str
    has_ad: bool = False
    has_uem: bool = False
    has_edr: bool = False
    has_asset: bool = False
    ad_os: str = ""
    uem_serial: str = ""
    uem_user: str = ""
    edr_serial: str = ""
    edr_user: str = ""
    last_seen_ms: int = 0
    last_seen_source: str = ""
    serial_is_cloned: bool = False
    is_virtual_gap: bool = False
    is_available_in_asset: bool = False


def _parse_date_ms(date_val: str) -> int:
    """Parse date string to epoch-ms. Returns 0 on failure."""
    if not date_val:
        return 0
    s = date_val.strip()
    import time as _time
    from datetime import datetime, timezone
    # ISO format: 2026-02-02T12:58:18Z
    try:
        if "T" in s and s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
    except Exception:
        pass
    # MM/DD/YYYY HH:MM (US format)
    try:
        m = re.match(
            r"^(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)?$",
            s, re.IGNORECASE,
        )
        if m:
            mo, day, yr = int(m.group(1)), int(m.group(2)), int(m.group(3))
            hr, mi = int(m.group(4)), int(m.group(5))
            sec = int(m.group(6)) if m.group(6) else 0
            ampm = (m.group(7) or "").upper()
            if ampm == "PM" and hr < 12:
                hr += 12
            if ampm == "AM" and hr == 12:
                hr = 0
            dt = datetime(yr, mo, day, hr, mi, sec, tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
    except Exception:
        pass
    return 0


# ──────────────────────────────────────────────────────────────────────────────
# Main builder
# ──────────────────────────────────────────────────────────────────────────────

def build_master_records(sources: MachinesSources) -> list[dict[str, Any]]:
    """
    Full join AD + UEM + EDR → master map.
    Apply ASSET lookup (marks has_asset, never creates new entries).
    Detect cloned serials.
    Returns List[Dict] consumable by MachineRecord(**dict) → MachinesEngine.

    Mirrors TS main() lines 3003-3198.
    """
    t0 = time.perf_counter()
    master: dict[str, _Entry] = {}

    # ── 1. AD (TS line 3095: for i=1..dataAD → upsert(..., "AD", idxAdName))
    # Columns: Computer Name → key; Operating System; Last Logon Time
    if sources.ad_df is not None:
        c_name = _col(sources.ad_df, "Computer Name", "ComputerName", "hostname")
        c_os   = _col(sources.ad_df, "Operating System", "OperatingSystem")
        c_logon = _col(sources.ad_df, "Last Logon Time", "LastLogonTime")
        n_new = 0
        if c_name:
            for row in sources.ad_df.to_dicts():
                raw = _val(row, c_name)
                if not raw:
                    continue
                key = normalize_key(raw)
                if not key:
                    continue
                if key not in master:
                    master[key] = _Entry(hostname=key, raw_name=raw, pa_code=extract_pa(key))
                    n_new += 1
                e = master[key]
                e.has_ad = True
                if c_os:
                    e.ad_os = _val(row, c_os)
                if c_logon:
                    ms = _parse_date_ms(_val(row, c_logon))
                    if ms > e.last_seen_ms:
                        e.last_seen_ms = ms
                        e.last_seen_source = "AD"
        log.info("AD upsert done: %d new entries (total=%d)", n_new, len(master))
    else:
        log.warning("AD DataFrame missing — skipping AD join")

    # ── 2. UEM (TS line 3096: for i=1..dataUEM → upsert(..., "UEM", idxUemName))
    # TS: idxUemName = getIdx(hUEM,"Hostname"); if -1 → getIdx(hUEM,"Friendly Name")
    if sources.uem_df is not None:
        c_name   = _col(sources.uem_df, "Hostname", "Friendly Name", "device_friendly_name")
        c_user   = _col(sources.uem_df, "Username")
        c_serial = _col(sources.uem_df, "Serial Number")
        c_seen   = _col(sources.uem_df, "Last Seen")
        n_new = 0
        if c_name:
            for row in sources.uem_df.to_dicts():
                raw = _val(row, c_name)
                if not raw:
                    continue
                key = normalize_key(raw)
                if not key:
                    continue
                if key not in master:
                    master[key] = _Entry(hostname=key, raw_name=raw, pa_code=extract_pa(key))
                    n_new += 1
                e = master[key]
                e.has_uem = True
                if c_user:   e.uem_user   = _val(row, c_user)
                if c_serial: e.uem_serial = _val(row, c_serial)
                seen_ms = _parse_date_ms(_val(row, c_seen) if c_seen else "")
                # TS: if lastSeenScore==0 and uemScore>0 → update
                if e.last_seen_ms == 0 and seen_ms > 0:
                    e.last_seen_ms = seen_ms
                    e.last_seen_source = "UEM"
        log.info("UEM upsert done: %d new entries (total=%d)", n_new, len(master))
    else:
        log.warning("UEM DataFrame missing — skipping UEM join")

    # ── 3. EDR (TS line 3097: for i=1..dataEDR → upsert(..., "EDR", idxEdrName))
    # TS: idxEdrName = getIdx(hEDR,"Friendly Name"); if -1 → getIdx(hEDR,"Hostname")
    if sources.edr_df is not None:
        c_name   = _col(sources.edr_df, "Friendly Name", "Hostname")
        c_user   = _col(sources.edr_df, "Last Logged In User Account")
        c_serial = _col(sources.edr_df, "Serial Number")
        c_seen   = _col(sources.edr_df, "Last Seen")
        n_new = 0
        if c_name:
            for row in sources.edr_df.to_dicts():
                raw = _val(row, c_name)
                if not raw:
                    continue
                key = normalize_key(raw)
                if not key:
                    continue
                if key not in master:
                    master[key] = _Entry(hostname=key, raw_name=raw, pa_code=extract_pa(key))
                    n_new += 1
                e = master[key]
                e.has_edr = True
                if c_user:   e.edr_user   = _val(row, c_user)
                if c_serial: e.edr_serial = _val(row, c_serial)
                seen_ms = _parse_date_ms(_val(row, c_seen) if c_seen else "")
                # TS: canOverrideWithEdr = !AD || score==0 || source=="UEM"
                can_override = (not e.has_ad) or (e.last_seen_ms == 0) or (e.last_seen_source in ("", "UEM"))
                if can_override and seen_ms > 0:
                    e.last_seen_ms = seen_ms
                    e.last_seen_source = "EDR"
        log.info("EDR upsert done: %d new entries (total=%d)", n_new, len(master))
    else:
        log.warning("EDR DataFrame missing — skipping EDR join")

    # ── 4. ASSET — lookup-only (TS lines 3196-3198)
    # Build asset_set from 'Nome do ativo' column → normalizeAssetHostname keys
    asset_set: set[str] = set()
    if sources.asset_df is not None:
        c_nome = _col(sources.asset_df, "Nome do ativo", "Nome do ativo ")
        if c_nome:
            for row in sources.asset_df.to_dicts():
                raw = _val(row, c_nome)
                if raw:
                    k = normalize_asset_key(raw)
                    if k:
                        asset_set.add(k)
        log.info("Asset set built: %d unique keys", len(asset_set))

    # Apply: mark has_asset on existing entries. NEVER add new ones.
    matched = 0
    for key, e in master.items():
        # TS: assetSet.has(k) || assetSet.has(normalizeSicFromExtra(k))
        m_match = re.match(r"^(SIC_\d+_\d+_\d+)", key, re.IGNORECASE)
        base = m_match.group(1).upper() if m_match else key
        if key in asset_set or base in asset_set:
            e.has_asset = True
            matched += 1
    log.info("Asset lookup: has_asset=True for %d / %d entries", matched, len(master))

    # ── 5. Clone detection
    serial_map: dict[str, list[str]] = {}
    for key, e in master.items():
        s = e.edr_serial or e.uem_serial
        if s and len(s) >= 5:
            serial_map.setdefault(s, []).append(key)
    cloned = {s for s, keys in serial_map.items() if len(keys) > 1}
    for key, e in master.items():
        s = e.edr_serial or e.uem_serial
        if s and s in cloned:
            e.serial_is_cloned = True
    log.info("Clone detection: %d cloned serials across %d entries", len(cloned), len(master))

    # ── 6. Convert to List[Dict] expected by MachineRecord(**dict)
    records = []
    for e in master.values():
        records.append({
            "hostname":               e.hostname,
            "pa_code":                e.pa_code,
            "has_ad":                 e.has_ad,
            "has_uem":                e.has_uem,
            "has_edr":                e.has_edr,
            "has_asset":              e.has_asset,
            "ad_os":                  e.ad_os or None,
            "uem_serial":             e.uem_serial or None,
            "edr_serial":             e.edr_serial or None,
            "main_user":              e.edr_user or e.uem_user or None,
            "uem_extra_user_logado":  None,
            "last_seen_date_ms":      e.last_seen_ms if e.last_seen_ms > 0 else None,
            "serial_is_cloned":       e.serial_is_cloned,
            "is_virtual_gap":         e.is_virtual_gap,
            "is_available_in_asset":  e.is_available_in_asset,
        })

    elapsed = round((time.perf_counter() - t0) * 1000, 1)
    log.info("MasterMap built: %d real machines in %sms", len(records), elapsed)
    return records
