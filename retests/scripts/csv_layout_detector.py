"""
csv_layout_detector.py — Detect header row and first data row for CSV files.

Implements the same header detection logic as dashboard_fixed.ts:
- AD/UEM/EDR: header is always row 0 (standard CSV)
- ASSET: header is detected dynamically by scanning for 'NOME DO ATIVO' (like loadAssetSet())
- Returns header_row_index, first_data_row_index, matched headers, and a confidence score.
"""

from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Any

# ──────────────────────────────────────────────────────────────────────────────
# Expected headers per source (mirrors getIdx() calls in dashboard_fixed.ts)
# ──────────────────────────────────────────────────────────────────────────────

EXPECTED_HEADERS: dict[str, dict[str, Any]] = {
    "AD": {
        "required": ["Computer Name", "Operating System"],
        "optional": ["Last Logon Time", "Password Last Set", "S.No", "DNS Name", "Version"],
        "identity_header": None,         # Standard CSV: row 0
        "scan_for": None,
    },
    "UEM": {
        "required": ["Friendly Name", "Last Seen", "Username", "Serial Number"],
        "optional": ["DM Last Seen", "OS", "Model", "Compliance Status", "Hostname"],
        "identity_header": None,         # Standard CSV: row 0
        "scan_for": None,
    },
    "EDR": {
        "required": ["Hostname", "Last Seen", "OS Version"],
        "optional": ["Last Logged In User Account", "Serial Number", "Sensor Tags", "Local IP"],
        "identity_header": None,         # Standard CSV: row 0
        "scan_for": None,
    },
    "ASSET": {
        "required": ["Nome do ativo"],
        "optional": ["Estado do ativo", "Produto", "Tipo de produto", "Departamento"],
        "identity_header": "NOME DO ATIVO",  # TS scans for this exact string
        "scan_for": "NOME DO ATIVO",
    },
}


def _norm(s: str) -> str:
    """Normalize a header string the same way TS does: strip BOM, NFD → remove accents, upper."""
    if not s:
        return ""
    s = s.strip().lstrip("\ufeff")
    s = unicodedata.normalize("NFD", s)
    s = re.sub(r"[\u0300-\u036f]", "", s)
    return s.upper()


def _read_raw_lines(path: Path, n: int = 50) -> list[str]:
    """Read first N raw lines using multiple encodings until one succeeds."""
    for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
        try:
            with open(path, "r", encoding=enc, errors="replace") as f:
                lines = [f.readline() for _ in range(n)]
            return [ln.rstrip("\r\n") for ln in lines]
        except Exception:
            continue
    return []


def _split_row(line: str) -> list[str]:
    """Try to split a CSV line preserving quoted fields."""
    import csv, io
    for sep in [",", ";"]:
        try:
            reader = csv.reader(io.StringIO(line), delimiter=sep)
            row = next(reader, [])
            if len(row) > 1:
                return [c.strip() for c in row]
        except Exception:
            pass
    return [line]


def _score_row(cells: list[str], expected: list[str]) -> int:
    """Count how many expected header names appear in this row (case-insensitive, accent-stripped)."""
    normed_cells = {_norm(c) for c in cells}
    return sum(1 for h in expected if _norm(h) in normed_cells)


def detect_header_row(
    path: Path,
    source_name: str,
    scan_rows: int = 50,
) -> dict[str, Any]:
    """
    Detect the header row and first data row for a CSV file.

    Returns a dict:
      {
        "source": str,
        "file": str,
        "detected_header_row_index": int,   # 0-based
        "detected_first_data_row_index": int,
        "matched_headers_sample": list[str],
        "confidence": float,  # 0.0 – 1.0
        "method": str,        # "scan_identity" | "best_match" | "row0_default"
        "raw_header_row": list[str],
      }
    """
    cfg = EXPECTED_HEADERS.get(source_name.upper(), {})
    required = cfg.get("required", [])
    optional = cfg.get("optional", [])
    all_expected = required + optional
    scan_for = cfg.get("scan_for")   # identity string to scan for (ASSET)

    raw_lines = _read_raw_lines(path, scan_rows)

    result = {
        "source": source_name,
        "file": path.name,
        "detected_header_row_index": 0,
        "detected_first_data_row_index": 1,
        "matched_headers_sample": [],
        "confidence": 0.0,
        "method": "row0_default",
        "raw_header_row": [],
    }

    if not raw_lines:
        return result

    # ── Strategy 1: identity scan (ASSET-style: scan for exact label)
    if scan_for:
        for i, line in enumerate(raw_lines):
            cells = _split_row(line)
            normed_cells = [_norm(c) for c in cells]
            if _norm(scan_for) in normed_cells:
                result["detected_header_row_index"] = i
                result["detected_first_data_row_index"] = i + 1
                result["method"] = "scan_identity"
                matched = [c for c in cells if _norm(c) in {_norm(h) for h in all_expected}]
                result["matched_headers_sample"] = matched
                result["confidence"] = 1.0 if _norm(scan_for) in normed_cells else 0.8
                result["raw_header_row"] = cells
                return result

    # ── Strategy 2: best-match row (compare all rows against expected headers)
    best_row_idx = 0
    best_score = 0
    best_cells: list[str] = []

    for i, line in enumerate(raw_lines):
        cells = _split_row(line)
        score = _score_row(cells, all_expected)
        if score > best_score:
            best_score = score
            best_row_idx = i
            best_cells = cells

    if best_score > 0:
        result["detected_header_row_index"] = best_row_idx
        result["detected_first_data_row_index"] = best_row_idx + 1
        result["method"] = "best_match" if best_row_idx > 0 else "row0_default"
        matched = [c for c in best_cells if _norm(c) in {_norm(h) for h in all_expected}]
        result["matched_headers_sample"] = matched
        confidence = min(1.0, best_score / max(len(required), 1))
        result["confidence"] = round(confidence, 2)
        result["raw_header_row"] = best_cells
    else:
        # Fallback: row 0
        cells = _split_row(raw_lines[0]) if raw_lines else []
        result["raw_header_row"] = cells
        result["confidence"] = 0.0
        result["method"] = "row0_default"

    return result


def generate_raw_head_artifact(
    path: Path,
    source_name: str,
    run_id: str,
    output_dir: Path,
    n: int = 30,
) -> Path:
    """Write the first N raw lines of the CSV to a .txt artifact."""
    raw_lines = _read_raw_lines(path, n)
    out_path = output_dir / f"{source_name}_raw_head_{n}_{run_id}.txt"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(f"# {source_name} — First {n} raw lines\n")
        f.write(f"# File: {path}\n\n")
        for i, line in enumerate(raw_lines):
            f.write(f"[{i:02d}] {line}\n")
    return out_path


def generate_detected_header_artifact(
    detection: dict[str, Any],
    run_id: str,
    output_dir: Path,
) -> Path:
    """Write the detection result as JSON artifact."""
    source = detection["source"]
    out_path = output_dir / f"{source}_detected_header_row_{run_id}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(detection, f, ensure_ascii=False, indent=2)
    return out_path


def detect_and_save_all(
    csv_paths: dict[str, Path],
    run_id: str,
    output_dir: Path,
) -> dict[str, dict[str, Any]]:
    """Run detection for all sources and save artifacts. Returns detections by source name."""
    output_dir.mkdir(parents=True, exist_ok=True)
    detections: dict[str, dict[str, Any]] = {}
    for source, path in csv_paths.items():
        if not path.exists():
            detections[source] = {"source": source, "error": "file_not_found"}
            continue
        # Raw head artifact
        generate_raw_head_artifact(path, source, run_id, output_dir)
        # Detection
        det = detect_header_row(path, source)
        detections[source] = det
        # Detection JSON artifact
        generate_detected_header_artifact(det, run_id, output_dir)
    return detections
