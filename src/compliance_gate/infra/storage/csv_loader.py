"""
csv_loader.py — Real CSV ingestion for the Machines domain.

Loads AD, UEM, EDR, and ASSET CSV files from a configurable data directory.
Mirrors the loading strategy of dashboard_fixed.ts:
  - AD/UEM/EDR: header at row 0 (standard CSV)
  - ASSET: header detected dynamically by scanning for 'NOME DO ATIVO'
  - Key normalization: strip everything from the first '.' → matches TS normalize()
  - ASSET is lookup-only: marks has_asset on existing records, never creates new ones.
"""

from __future__ import annotations

import logging
import os
import re
import time
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import polars as pl

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Config helpers
# ──────────────────────────────────────────────────────────────────────────────

def _get_data_dir() -> Path:
    """
    Resolve data directory from env var CG_DATA_DIR.
    Falls back to the project root (two dirs above this file: src/compliance_gate/infra/storage/).
    In Docker: CG_DATA_DIR=/workspace where CSVs are mounted read-only.
    """
    env = os.environ.get("CG_DATA_DIR", "")
    if env and Path(env).exists():
        return Path(env)
    # Heuristic: walk up to find a directory containing AD.csv / AD.CSV
    candidates = [
        Path(__file__).resolve().parents[4],   # project root if installed at src/...
        Path("/workspace"),
    ]
    for c in candidates:
        if c.exists() and any((c / f).exists() for f in ["AD.csv", "AD.CSV"]):
            return c
    # last resort: cwd
    return Path.cwd()


def _find_file(data_dir: Path, name: str) -> Optional[Path]:
    """Case-insensitive file search in data_dir."""
    direct = data_dir / name
    if direct.exists():
        return direct
    for p in data_dir.iterdir():
        if p.name.upper() == name.upper():
            return p
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Robust CSV reader
# ──────────────────────────────────────────────────────────────────────────────

def _read_csv_robust(path: Path, skip_rows: int = 0) -> Optional[pl.DataFrame]:
    """Try multiple delimiters and encodings. Returns None if all fail."""
    for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
        for sep in [",", ";"]:
            try:
                df = pl.read_csv(
                    path,
                    separator=sep,
                    encoding=enc,
                    skip_rows=skip_rows,
                    infer_schema_length=500,
                    ignore_errors=True,
                    truncate_ragged_lines=True,
                )
                if df.shape[1] > 1:
                    return df
            except Exception:
                continue
    return None


# ──────────────────────────────────────────────────────────────────────────────
# ASSET header detection (mirrors loadAssetSet in dashboard_fixed.ts line 2839)
# ──────────────────────────────────────────────────────────────────────────────

def _find_asset_header_row(path: Path) -> int:
    """
    Scan raw lines for 'NOME DO ATIVO' — identical to TS loadAssetSet() lines 2843-2854.
    Returns the 0-based row index of the header row.
    """
    for enc in ["utf-8-sig", "utf-8", "latin-1"]:
        try:
            with open(path, "r", encoding=enc, errors="replace") as f:
                for i, line in enumerate(f):
                    if "NOME DO ATIVO" in line.upper():
                        log.debug("ASSET header found at row %d (enc=%s)", i, enc)
                        return i
        except Exception:
            pass
    log.warning("ASSET: 'NOME DO ATIVO' not found in first scan — falling back to row 4")
    return 4  # observed structure: 4 metadata rows then header


# ──────────────────────────────────────────────────────────────────────────────
# Sources container
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class MachinesSources:
    ad_df: Optional[pl.DataFrame] = None
    uem_df: Optional[pl.DataFrame] = None
    edr_df: Optional[pl.DataFrame] = None
    asset_df: Optional[pl.DataFrame] = None
    data_dir: Path = field(default_factory=Path.cwd)
    load_errors: list[str] = field(default_factory=list)


def load_machines_sources(data_dir: Optional[Path] = None) -> MachinesSources:
    """
    Load all four CSV sources for the Machines domain.
    AD/UEM/EDR: header at row 0.
    ASSET: header detected dynamically.
    """
    if data_dir is None:
        data_dir = _get_data_dir()

    sources = MachinesSources(data_dir=data_dir)
    t0 = time.perf_counter()

    # ── AD ────────────────────────────────────────────────────────────────────
    ad_path = _find_file(data_dir, "AD.csv")
    if ad_path:
        df = _read_csv_robust(ad_path, skip_rows=0)
        if df is not None:
            sources.ad_df = df
            log.info("Loaded AD: rows=%d cols=%d  path=%s", df.height, df.width, ad_path.name)
        else:
            msg = f"AD.csv parse failed at {ad_path}"
            log.error(msg)
            sources.load_errors.append(msg)
    else:
        msg = f"AD.csv not found in {data_dir}"
        log.warning(msg)
        sources.load_errors.append(msg)

    # ── UEM ───────────────────────────────────────────────────────────────────
    uem_path = _find_file(data_dir, "UEM.csv")
    if uem_path:
        df = _read_csv_robust(uem_path, skip_rows=0)
        if df is not None:
            sources.uem_df = df
            log.info("Loaded UEM: rows=%d cols=%d  path=%s", df.height, df.width, uem_path.name)
        else:
            msg = f"UEM.csv parse failed at {uem_path}"
            log.error(msg)
            sources.load_errors.append(msg)
    else:
        msg = f"UEM.csv not found in {data_dir}"
        log.warning(msg)
        sources.load_errors.append(msg)

    # ── EDR ───────────────────────────────────────────────────────────────────
    edr_path = _find_file(data_dir, "EDR.csv")
    if edr_path:
        df = _read_csv_robust(edr_path, skip_rows=0)
        if df is not None:
            sources.edr_df = df
            log.info("Loaded EDR: rows=%d cols=%d  path=%s", df.height, df.width, edr_path.name)
        else:
            msg = f"EDR.csv parse failed at {edr_path}"
            log.error(msg)
            sources.load_errors.append(msg)
    else:
        msg = f"EDR.csv not found in {data_dir}"
        log.warning(msg)
        sources.load_errors.append(msg)

    # ── ASSET (lookup-only) ───────────────────────────────────────────────────
    asset_path = _find_file(data_dir, "ASSET.CSV") or _find_file(data_dir, "ASSET.csv")
    if asset_path:
        skip = _find_asset_header_row(asset_path)
        df = _read_csv_robust(asset_path, skip_rows=skip)
        if df is not None:
            sources.asset_df = df
            log.info(
                "Loaded ASSET: rows=%d cols=%d  header_row=%d  path=%s",
                df.height, df.width, skip, asset_path.name,
            )
        else:
            msg = f"ASSET.CSV parse failed at {asset_path}"
            log.error(msg)
            sources.load_errors.append(msg)
    else:
        msg = f"ASSET.CSV not found in {data_dir}"
        log.warning(msg)
        sources.load_errors.append(msg)

    elapsed = round((time.perf_counter() - t0) * 1000, 1)
    log.info("CSV load complete in %sms — errors=%d", elapsed, len(sources.load_errors))
    return sources
