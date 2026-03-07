"""
csv_reader.py — Robust CSV reader for Compliance Gate ingest pipeline.

Improvements over the original csv_loader.py:
  - BOM strip before comparing column names (mirrors TS getIdx BOM removal)
  - csv.Sniffer-based delimiter detection before Polars attempt
  - Typed result: CsvReadResult with detected metadata
  - Header-first validation: returns detected_headers list
  - All comparison is exact: upper/trim/BOM-strip (no fuzzy matching)
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import polars as pl

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Result types
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CsvReadResult:
    """Typed result of a single CSV read attempt."""
    source: str                         # e.g. "AD", "UEM"
    path: Path
    df: Optional[pl.DataFrame]
    detected_encoding: str = "unknown"
    detected_delimiter: str = ","
    header_row_index: int = 0           # 0-based row containing headers
    detected_headers: list[str] = field(default_factory=list)
    rows_read: int = 0
    checksum_sha256: str = ""
    file_size_bytes: int = 0
    warnings: list[str] = field(default_factory=list)
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.df is not None and self.error is None

    @property
    def cols(self) -> int:
        return self.df.width if self.df is not None else 0


# ─────────────────────────────────────────────────────────────────────────────
# BOM / key normalisation (mirrors TS getIdx)
# ─────────────────────────────────────────────────────────────────────────────

_BOM = "\ufeff"

def normalize_header_key(raw: str) -> str:
    """upper + strip + remove BOM — identical to TS getIdx inner logic."""
    return raw.strip().lstrip(_BOM).upper()


# ─────────────────────────────────────────────────────────────────────────────
# File helpers
# ─────────────────────────────────────────────────────────────────────────────

def _sha256(path: Path, chunk: int = 65536) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except Exception:
        return 0


ENCODINGS = ["utf-8-sig", "utf-8", "latin-1", "cp1252"]
DELIMITERS = [",", ";", "\t"]


# ─────────────────────────────────────────────────────────────────────────────
# Sniffer-based delimiter detection
# ─────────────────────────────────────────────────────────────────────────────

def _sniff_delimiter(path: Path, enc: str) -> Optional[str]:
    """Use csv.Sniffer on the first 8KB to guess delimiter."""
    try:
        with open(path, "r", encoding=enc, errors="replace") as f:
            sample = f.read(8192)
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return dialect.delimiter
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Core reader
# ─────────────────────────────────────────────────────────────────────────────

def _read_polars(
    path: Path,
    sep: str,
    enc: str,
    skip_rows: int,
) -> Optional[pl.DataFrame]:
    """Single polars attempt. Returns None on any failure."""
    try:
        df = pl.read_csv(
            path,
            separator=sep,
            encoding=enc,
            skip_rows=skip_rows,
            infer_schema_length=0, # Reads all columns purely as text/strings
            quote_char='"',        # Respects internal quotes for escaping delimiters/newlines
            ignore_errors=True,
            truncate_ragged_lines=True,
        )
        if df.shape[1] > 1 and df.shape[0] > 0:
            return df
    except Exception:
        pass
    return None


def _try_all_combinations(
    path: Path,
    skip_rows: int,
    preferred_delimiter: Optional[str] = None,
) -> tuple[Optional[pl.DataFrame], str, str]:
    """
    Try enc × sep combinations. Preferred delimiter checked first.
    Returns (df, encoding, delimiter) — df may be None.
    """
    delimiters = DELIMITERS.copy()
    if preferred_delimiter and preferred_delimiter in delimiters:
        delimiters.remove(preferred_delimiter)
        delimiters.insert(0, preferred_delimiter)

    for enc in ENCODINGS:
        for sep in delimiters:
            df = _read_polars(path, sep, enc, skip_rows)
            if df is not None:
                return df, enc, sep

    return None, "unknown", ","


# ─────────────────────────────────────────────────────────────────────────────
# ASSET header scan (mirrors TS loadAssetSet lines 2843–2854)
# ─────────────────────────────────────────────────────────────────────────────

_ASSET_KEYWORD = "NOME DO ATIVO"


def find_asset_header_row(path: Path) -> tuple[int, str]:
    """
    Scan lines for 'NOME DO ATIVO'. Returns (0-based row index, encoding used).
    Falls back to row 4 if not found.
    """
    for enc in ENCODINGS:
        try:
            with open(path, "r", encoding=enc, errors="replace") as f:
                for i, line in enumerate(f):
                    if _ASSET_KEYWORD in normalize_header_key(line):
                        log.debug("ASSET header found at row %d (enc=%s)", i, enc)
                        return i, enc
        except Exception:
            continue

    log.warning("ASSET: '%s' not found — falling back to row 4", _ASSET_KEYWORD)
    return 4, "utf-8-sig"


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def read_csv(
    path: Path,
    source: str,
    skip_rows: int = 0,
    hint_delimiter: Optional[str] = None,
    force_delimiter: Optional[str] = None,
    force_encoding: Optional[str] = None,
) -> CsvReadResult:
    """
    Read a CSV file robustly. Returns a CsvReadResult with full metadata.

    Args:
        path:           Absolute path to the CSV file.
        source:         Source label for logging ("AD", "UEM", etc.)
        skip_rows:      0-based rows to skip before reading header.
        hint_delimiter: Optional pre-sniffed delimiter (from Sniffer).
    """
    result = CsvReadResult(source=source, path=path, df=None)

    if not path.exists():
        result.error = f"{source}: file not found at {path}"
        return result

    result.file_size_bytes = _file_size(path)

    t0 = time.perf_counter()

    # 1. Checksum
    try:
        result.checksum_sha256 = _sha256(path)
    except Exception as e:
        result.warnings.append(f"checksum failed: {e}")

    # 2. Sniff delimiter (use first encoding that works)
    sniffed: Optional[str] = force_delimiter or hint_delimiter
    if not sniffed:
        encs_to_try = [force_encoding] if force_encoding else ENCODINGS
        for enc in encs_to_try:
            sniffed = _sniff_delimiter(path, enc)
            if sniffed:
                result.detected_encoding = enc
                break

    result.header_row_index = skip_rows

    # 3. Try all enc/sep combos
    df = None
    enc_used = force_encoding or "unknown"
    sep_used = force_delimiter or ","

    if force_encoding and force_delimiter:
        # Both forced
        df = _read_polars(path, force_delimiter, force_encoding, skip_rows)
    elif force_encoding:
        # Encoding forced, sniff delimiter
        df, _, sep_used = _try_all_combinations(path, skip_rows, sniffed)
        enc_used = force_encoding
    elif force_delimiter:
        # Delimiter forced, try encodings
        df, enc_used, _ = _try_all_combinations(path, skip_rows, force_delimiter)
        sep_used = force_delimiter
    else:
        # Standard fallback matrix
        df, enc_used, sep_used = _try_all_combinations(path, skip_rows, sniffed)

    if df is None:
        result.error = f"{source}: could not parse CSV at {path} (tried all enc/sep combos)"
        log.error(result.error)
        return result

    result.df = df
    result.detected_encoding = enc_used
    result.detected_delimiter = sep_used
    result.rows_read = df.height
    result.detected_headers = [normalize_header_key(c) for c in df.columns]

    elapsed = round((time.perf_counter() - t0) * 1000, 1)
    log.info(
        "%s: read OK  rows=%d cols=%d enc=%s sep=%r  %.1fms",
        source, df.height, df.width, enc_used, sep_used, elapsed,
    )
    return result


from compliance_gate.domains.machines.ingest.mapping_profile import CsvTabConfig

def read_csv_for_source(
    source: str,
    data_dir: Path,
    filename_candidates: list[str],
    is_asset: bool = False,
    config: Optional[CsvTabConfig] = None,
) -> CsvReadResult:
    """
    Find and read a CSV source from a data directory.
    Handles case-insensitive file discovery and ASSET dynamic header detection.
    """
    # Case-insensitive file lookup
    found_path: Optional[Path] = None
    for candidate in filename_candidates:
        p = data_dir / candidate
        if p.exists():
            found_path = p
            break
        # Case-insensitive scan
        for existing in data_dir.iterdir():
            if existing.name.upper() == candidate.upper():
                found_path = existing
                break
        if found_path:
            break

    if found_path is None:
        r = CsvReadResult(source=source, path=data_dir / filename_candidates[0], df=None)
        r.error = f"{source}: not found in {data_dir} (tried {filename_candidates})"
        log.warning(r.error)
        return r

    skip = config.header_row if config else 0
    if is_asset and not config:
        skip, _ = find_asset_header_row(found_path)

    return read_csv(
        found_path,
        source,
        skip_rows=skip,
        force_delimiter=config.delimiter if config else None,
        force_encoding=config.encoding if config else None,
    )
