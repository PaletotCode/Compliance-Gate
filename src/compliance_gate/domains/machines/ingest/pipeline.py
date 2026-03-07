"""
pipeline.py — Ingest orchestrator for the Machines domain.

Coordinates: load CSVs → validate headers → build master records → collect metrics.
Does NOT write to DB — caller (the HTTP handler) commits after receiving IngestResult.

Designed to be called from:
  - POST /datasets/machines/ingest  (persists)
  - POST /datasets/machines/preview (does not persist)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from compliance_gate.domains.machines.ingest.mapping_profile import CsvTabConfig
from compliance_gate.domains.machines.ingest.schema_registry import (
    SCHEMAS,
    resolve_col,
    validate_schema,
)
from compliance_gate.domains.machines.ingest.sources import (
    MACHINES_SOURCES,
    SourceDefinition,
)
from compliance_gate.infra.storage.csv_reader import CsvReadResult, read_csv_for_source
from compliance_gate.shared.observability.metrics import (
    IngestMetrics,
    JoinMetrics,
    ParseMetrics,
)
from compliance_gate.shared.observability.logger import log_ingest_event, log_parse_warning

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Result types
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class FileIngested:
    """Metadata about one loaded source file."""
    source: str
    read_result: CsvReadResult
    missing_required: list[str] = field(default_factory=list)
    missing_optional: list[str] = field(default_factory=list)


@dataclass
class IngestResult:
    """
    Full result of one ingest pipeline run.
    records: List[Dict] ready for MachineRecord(**dict) → MachinesEngine
    """
    records: list[dict[str, Any]]
    metrics: IngestMetrics
    files: list[FileIngested]
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return bool(self.records)

    def summary(self) -> dict:
        return {
            "total_records": len(self.records),
            "warnings": len(self.warnings),
            "metrics": self.metrics.to_dict(),
            "files": [
                {
                    "source": f.source,
                    "ok": f.read_result.ok,
                    "rows": f.read_result.rows_read,
                    "missing_required": f.missing_required,
                }
                for f in self.files
            ],
        }


# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline function
# ─────────────────────────────────────────────────────────────────────────────

def run_ingest_pipeline(
    data_dir: Path,
    *,
    dataset_version_id: str = "preview",
    configs: Optional[dict[str, CsvTabConfig]] = None,
) -> IngestResult:
    """
    Load all CSV sources, validate headers, build master records, collect metrics.

    Args:
        data_dir:           Directory containing AD.csv, UEM.csv, EDR.csv, ASSET.CSV
        dataset_version_id: Used to tag IngestMetrics (pass "preview" for dry-runs)
        configs:            Dict mapping source (AD, UEM) to CsvTabConfig properties
    """
    t_total = time.perf_counter()
    configs = configs or {}
    all_warnings: list[str] = []
    files_ingested: list[FileIngested] = []
    parse_metrics: list[ParseMetrics] = []

    log_ingest_event("pipeline_start", f"Starting ingest from {data_dir}",
                     {"data_dir": str(data_dir), "version_id": dataset_version_id})

    # ── 1. Load all sources ──────────────────────────────────────────────────
    read_results: dict[str, CsvReadResult] = {}
    for src_def in MACHINES_SOURCES:
        t0 = time.perf_counter()
        cfg = configs.get(src_def.name)
        
        r = read_csv_for_source(
            source=src_def.name,
            data_dir=data_dir,
            filename_candidates=src_def.filename_candidates,
            is_asset=(src_def.header_strategy == "scan_keyword"),
            config=cfg,
        )
        elapsed = round((time.perf_counter() - t0) * 1000, 1)

        pm = ParseMetrics(
            source=src_def.name,
            rows_read=r.rows_read,
            rows_valid=r.rows_read,  # refined below during validation
            elapsed_ms=elapsed,
            warnings=list(r.warnings),
        )

        fi = FileIngested(source=src_def.name, read_result=r)

        if not r.ok:
            w = r.error or f"{src_def.name}: load failed"
            all_warnings.append(w)
            pm.warnings.append(w)
            log_ingest_event("load_error", w, {"source": src_def.name}, level="WARNING")
        else:
            # ── 1.5 Apply Config Overrides before Schema Validation ────────
            schema = SCHEMAS.get(src_def.name)
            if cfg and schema and r.df is not None:
                # Inject sic_column alias as the primary expected column name
                # This tricks master_map_builder and schema_registry to pass validation
                canonical_key = schema.required[0].canonical
                if cfg.sic_column in r.df.columns and cfg.sic_column != canonical_key:
                    # Rename the chosen sic_column to the internal canonical key
                    if canonical_key in r.df.columns:
                        r.df = r.df.drop(canonical_key)
                    r.df = r.df.rename({cfg.sic_column: canonical_key})

            # ── 2. Validate headers ─────────────────────────────────────────
            if schema and r.df is not None:
                missing_req, missing_opt = validate_schema(r.df, schema)
                fi.missing_required = missing_req
                fi.missing_optional = missing_opt

                if missing_req:
                    w = f"{src_def.name}: missing required columns: {missing_req}"
                    all_warnings.append(w)
                    pm.warnings.append(w)
                    log_ingest_event("schema_error", w, {"source": src_def.name, "missing": missing_req}, level="WARNING")

                if missing_opt:
                    log_ingest_event("schema_warn", f"{src_def.name}: missing optional cols",
                                     {"source": src_def.name, "optional_missing": missing_opt})

            log_ingest_event("load_ok", f"{src_def.name} loaded",
                             {"source": src_def.name, "rows": r.rows_read,
                              "enc": r.detected_encoding, "sep": repr(r.detected_delimiter),
                              "header_row": r.header_row_index})

        read_results[src_def.name] = r
        parse_metrics.append(pm)
        files_ingested.append(fi)

    # ── 3. Build master records (via existing master_map_builder) ─────────────
    from compliance_gate.infra.storage.csv_loader import MachinesSources
    from compliance_gate.domains.machines.master_map_builder import build_master_records

    sources = MachinesSources(
        ad_df=read_results["AD"].df if read_results["AD"].ok else None,
        uem_df=read_results["UEM"].df if read_results["UEM"].ok else None,
        edr_df=read_results["EDR"].df if read_results["EDR"].ok else None,
        asset_df=read_results["ASSET"].df if read_results["ASSET"].ok else None,
        data_dir=data_dir,
    )

    t_join = time.perf_counter()
    records = build_master_records(sources)
    join_elapsed = round((time.perf_counter() - t_join) * 1000, 1)

    # ── 4. Collect join metrics ───────────────────────────────────────────────
    jm = _collect_join_metrics(records, join_elapsed)

    total_elapsed = round((time.perf_counter() - t_total) * 1000, 1)

    metrics = IngestMetrics(
        dataset_version_id=dataset_version_id,
        parse=parse_metrics,
        join=jm,
        total_elapsed_ms=total_elapsed,
        warnings=all_warnings,
    )

    log_ingest_event("pipeline_done",
                     f"Ingest complete: {len(records)} records in {total_elapsed}ms",
                     {"records": len(records), "warnings": len(all_warnings),
                      "elapsed_ms": total_elapsed})

    return IngestResult(
        records=records,
        metrics=metrics,
        files=files_ingested,
        warnings=all_warnings,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Metrics collection helpers
# ─────────────────────────────────────────────────────────────────────────────

def _collect_join_metrics(records: list[dict[str, Any]], elapsed_ms: float) -> JoinMetrics:
    """Derive join quality metrics from the list of master records."""
    jm = JoinMetrics(total_entries=len(records), elapsed_ms=elapsed_ms)
    for r in records:
        if r.get("has_ad"):
            jm.from_ad += 1
        if r.get("has_uem"):
            jm.from_uem += 1
        if r.get("has_edr"):
            jm.from_edr += 1
        if r.get("has_ad") and r.get("has_uem"):
            jm.match_ad_uem += 1
        if r.get("has_ad") and r.get("has_edr"):
            jm.match_ad_edr += 1
        if r.get("has_asset"):
            jm.asset_matched += 1
        if r.get("serial_is_cloned"):
            jm.cloned_serials += 1
    return jm
