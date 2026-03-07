"""
metrics.py — Typed dataclasses for ingest observability.

ParseMetrics: per-source CSV parsing statistics
JoinMetrics: result of the AD+UEM+EDR master map join
IngestMetrics: full run aggregate (passed to datasets_store)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ParseMetrics:
    """Metrics for one CSV source (AD / UEM / EDR / ASSET)."""
    source: str
    rows_read: int = 0
    rows_valid: int = 0
    date_parse_ok: int = 0
    date_parse_fail: int = 0
    elapsed_ms: float = 0.0
    warnings: list[str] = field(default_factory=list)

    @property
    def parse_rate(self) -> float:
        return self.rows_valid / self.rows_read if self.rows_read > 0 else 0.0

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "rows_read": self.rows_read,
            "rows_valid": self.rows_valid,
            "parse_rate": round(self.parse_rate, 4),
            "date_parse_ok": self.date_parse_ok,
            "date_parse_fail": self.date_parse_fail,
            "elapsed_ms": self.elapsed_ms,
            "warnings": self.warnings,
        }


@dataclass
class JoinMetrics:
    """Metrics from the AD+UEM+EDR master map build."""
    total_entries: int = 0
    from_ad: int = 0
    from_uem: int = 0
    from_edr: int = 0
    match_ad_uem: int = 0     # entries present in both AD and UEM
    match_ad_edr: int = 0     # entries present in both AD and EDR
    asset_matched: int = 0    # entries with has_asset=True
    cloned_serials: int = 0
    elapsed_ms: float = 0.0

    @property
    def match_rate(self) -> float:
        """Fraction of entries that appear in more than one source."""
        cross = self.match_ad_uem + self.match_ad_edr
        return cross / self.total_entries if self.total_entries > 0 else 0.0

    def to_dict(self) -> dict:
        return {
            "total_entries": self.total_entries,
            "from_ad": self.from_ad,
            "from_uem": self.from_uem,
            "from_edr": self.from_edr,
            "match_ad_uem": self.match_ad_uem,
            "match_ad_edr": self.match_ad_edr,
            "match_rate": round(self.match_rate, 4),
            "asset_matched": self.asset_matched,
            "cloned_serials": self.cloned_serials,
            "elapsed_ms": self.elapsed_ms,
        }


@dataclass
class IngestMetrics:
    """Full run aggregate — persisted to dataset_metrics table."""
    dataset_version_id: str
    parse: list[ParseMetrics] = field(default_factory=list)
    join: Optional[JoinMetrics] = None
    total_elapsed_ms: float = 0.0
    warnings: list[str] = field(default_factory=list)

    @property
    def rows_read_total(self) -> int:
        return sum(p.rows_read for p in self.parse)

    @property
    def rows_valid_total(self) -> int:
        return sum(p.rows_valid for p in self.parse)

    def to_dict(self) -> dict:
        return {
            "dataset_version_id": self.dataset_version_id,
            "parse": [p.to_dict() for p in self.parse],
            "join": self.join.to_dict() if self.join else None,
            "rows_read_total": self.rows_read_total,
            "rows_valid_total": self.rows_valid_total,
            "total_elapsed_ms": self.total_elapsed_ms,
            "warnings": self.warnings,
        }


def record_http_request(method: str, path: str, status: int, duration: float) -> None:
    """Stub — kept for backward compatibility."""
    pass
