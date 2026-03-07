"""
preview.py — Preview helpers for the Machines ingest domain.

- preview_raw: reads a file with minimal config (header_row guess) and returns un-parsed top rows.
- preview_parsed: applies a CsvTabConfig and returns how the data will look after alias/key mapping.
- run_preview: dry-runs the full ingest pipeline (deprecated for UI, kept for tests).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
import polars as pl

from compliance_gate.domains.machines.ingest.mapping_profile import CsvTabConfig
from compliance_gate.domains.machines.ingest.pipeline import IngestResult, run_ingest_pipeline
from compliance_gate.domains.machines.ingest.schema_registry import normalize_col
from compliance_gate.infra.storage.csv_reader import read_csv, read_csv_for_source, CsvReadResult
from compliance_gate.shared.observability.logger import log_ingest_event


@dataclass
class SourceLayout:
    """Detected layout for one CSV source."""
    source: str
    filename: Optional[str]
    exists: bool
    detected_encoding: str = "unknown"
    detected_delimiter: str = ","
    header_row_index: int = 0
    headers: list[str] = field(default_factory=list)
    rows: int = 0
    checksum_sha256: Optional[str] = None
    missing_required: list[str] = field(default_factory=list)
    missing_optional: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class PreviewResult:
    """Result of a dry-run preview: no data written to DB."""
    layouts: list[SourceLayout]
    sample_rows: list[dict[str, Any]]          # up to 50 records from master map
    parse_metrics: list[dict]
    join_metrics: Optional[dict]
    total_elapsed_ms: float
    warnings: list[str]

    @property
    def ok(self) -> bool:
        return any(l.exists for l in self.layouts)

    def to_response(self) -> dict:
        return {
            "status": "ok" if self.ok else "error",
            "layouts": [
                {
                    "source": l.source,
                    "filename": l.filename,
                    "exists": l.exists,
                    "detected_encoding": l.detected_encoding,
                    "detected_delimiter": l.detected_delimiter,
                    "header_row_index": l.header_row_index,
                    "headers": l.headers,
                    "rows": l.rows,
                    "checksum_sha256": l.checksum_sha256,
                    "missing_required": l.missing_required,
                    "missing_optional": l.missing_optional,
                    "warnings": l.warnings,
                }
                for l in self.layouts
            ],
            "sample_rows": self.sample_rows,
            "parse_metrics": self.parse_metrics,
            "join_metrics": self.join_metrics,
            "total_elapsed_ms": self.total_elapsed_ms,
            "warnings": self.warnings,
        }


def run_preview(
    data_dir: Path,
    *,
    profile_id: Optional[str] = None,
    max_samples: int = 50,
) -> PreviewResult:
    """
    Dry-run the full ingest pipeline. Returns PreviewResult without writing to DB.
    """
    t0 = time.perf_counter()
    log_ingest_event("preview_start", f"Preview from {data_dir}")

    result: IngestResult = run_ingest_pipeline(
        data_dir,
        dataset_version_id="preview",
        profile_ids={},  # Full Ingest uses profile_ids dict now. Kept empty for old tests.
    )

    layouts: list[SourceLayout] = []
    for fi in result.files:
        r = fi.read_result
        layouts.append(SourceLayout(
            source=fi.source,
            filename=r.path.name if r.path else None,
            exists=r.ok,
            detected_encoding=r.detected_encoding,
            detected_delimiter=r.detected_delimiter,
            header_row_index=r.header_row_index,
            headers=r.detected_headers,
            rows=r.rows_read,
            checksum_sha256=r.checksum_sha256 or None,
            missing_required=fi.missing_required,
            missing_optional=fi.missing_optional,
            warnings=r.warnings,
        ))

    sample_rows: list[dict[str, Any]] = []
    for rec in result.records[:max_samples]:
        safe: dict[str, Any] = {}
        for k, v in rec.items():
            if isinstance(v, str) and len(v) > 200:
                safe[k] = v[:200] + "..."
            else:
                safe[k] = v
        sample_rows.append(safe)

    total_elapsed = round((time.perf_counter() - t0) * 1000, 1)
    log_ingest_event("preview_done", f"Preview done: {len(result.records)} records",
                     {"records": len(result.records), "elapsed_ms": total_elapsed})

    return PreviewResult(
        layouts=layouts,
        sample_rows=sample_rows,
        parse_metrics=[pm.to_dict() for pm in result.metrics.parse],
        join_metrics=result.metrics.join.to_dict() if result.metrics.join else None,
        total_elapsed_ms=total_elapsed,
        warnings=result.warnings,
    )


def preview_raw(
    source: str,
    file_path: Path,
    header_row_override: Optional[int] = None,
    delimiter_override: Optional[str] = None,
    encoding_override: Optional[str] = None,
    max_samples: int = 15,
) -> dict:
    """Reads the raw CSV file using optional overrides, returning the plain detected layout."""
    t0 = time.perf_counter()

    res = read_csv(
        path=file_path,
        source=source,
        skip_rows=header_row_override or 0,
        force_delimiter=delimiter_override,
        force_encoding=encoding_override,
    )

    sample_rows = []
    if res.ok and res.df is not None:
        raw_rows = res.df.head(max_samples).to_dicts()
        for rec in raw_rows:
            safe_rec = {k: (v[:200] + "..." if isinstance(v, str) and len(v) > 200 else v) for k, v in rec.items()}
            sample_rows.append(safe_rec)

    total_elapsed = round((time.perf_counter() - t0) * 1000, 1)

    return {
        "status": "ok" if res.ok else "error",
        "source": source,
        "exists": file_path.exists(),
        "detected_encoding": res.detected_encoding,
        "detected_delimiter": res.detected_delimiter,
        "header_row_index": res.header_row_index,
        "detected_headers": res.detected_headers,
        "original_headers": res.df.columns if res.df is not None else [],
        "rows_total_read": res.rows_read,
        "sample_rows": sample_rows,
        "warnings": res.warnings,
        "error": res.error,
        "elapsed_ms": total_elapsed,
    }

def preview_parsed(
    source: str,
    file_path: Path,
    config: CsvTabConfig,
    max_samples: int = 15,
) -> dict:
    """
    Reads the CSV and applies the CsvTabConfig logic (aliasing, key extraction),
    returning how the parsed records will look before join.
    """
    t0 = time.perf_counter()

    res = read_csv(
        path=file_path,
        source=source,
        skip_rows=config.header_row,
        force_delimiter=config.delimiter,
        force_encoding=config.encoding,
    )

    sample_rows = []
    warnings = list(res.warnings)
    
    if res.ok and res.df is not None:
        df = res.df
        orig_cols = df.columns
        
        # Validate sic_column exists
        if config.sic_column not in orig_cols:
            warnings.append(f"sic_column '{config.sic_column}' not found in file headers.")
            sic_mock_available = False
        else:
            sic_mock_available = True

        raw_rows = df.head(max_samples).to_dicts()
        for rec in raw_rows:
            safe_rec = {}
            # Include only selected cols
            for k in config.selected_columns:
                if k in rec:
                    v = rec[k]
                    safe_rec[k] = (v[:200] + "..." if isinstance(v, str) and len(v) > 200 else v)
            
            # Inject fake canonical sic column for preview
            if sic_mock_available:
                raw_sic = rec.get(config.sic_column, "")
                # Simulate normalization locally
                safe_rec["__sic_preview__"] = normalize_col(raw_sic) if raw_sic else None

            sample_rows.append(safe_rec)

    total_elapsed = round((time.perf_counter() - t0) * 1000, 1)

    return {
        "status": "ok" if res.ok else "error",
        "source": source,
        "config_applied": config.model_dump(),
        "sample_rows": sample_rows,
        "warnings": warnings,
        "error": res.error,
        "elapsed_ms": total_elapsed,
    }
