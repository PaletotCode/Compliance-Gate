"""
sources.py — Declarative source definitions for the Machines ingest domain.

Each SourceDefinition describes how to locate and read one CSV source.
This replaces magic strings scattered across csv_loader.py and master_map_builder.py
with a single, testable registry.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Optional


HeaderStrategy = Literal["fixed_0", "scan_keyword"]


@dataclass(frozen=True)
class SourceDefinition:
    """
    Describes one CSV source file for the Machines domain.

    name:                   Canonical label (AD / UEM / EDR / ASSET)
    filename_candidates:    Filenames to look for, in preference order (case-insensitive)
    is_lookup_only:         If True, never creates new entries in master map (ASSET)
    key_col_aliases:        Column names to try as the primary hostname key, in order.
                            Mirrors TS getIdx with fallback (e.g. UEM tries Hostname then Friendly Name)
    header_strategy:        "fixed_0" = header is always row 0;
                            "scan_keyword" = scan lines for scan_keyword to find header row
    scan_keyword:           Required when header_strategy == "scan_keyword"
    """
    name: str
    filename_candidates: list[str]
    is_lookup_only: bool = False
    key_col_aliases: list[str] = field(default_factory=list)
    header_strategy: HeaderStrategy = "fixed_0"
    scan_keyword: Optional[str] = None


# ─────────────────────────────────────────────────────────────────────────────
# Source registry — mirrors dashboard_fixed.ts join order
# ─────────────────────────────────────────────────────────────────────────────

AD = SourceDefinition(
    name="AD",
    filename_candidates=["AD.csv", "AD.CSV"],
    is_lookup_only=False,
    # TS line 2941: idxAdName = getIdx(hAD, "Computer Name")
    key_col_aliases=["Computer Name", "ComputerName", "hostname"],
    header_strategy="fixed_0",
)

UEM = SourceDefinition(
    name="UEM",
    filename_candidates=["UEM.csv", "UEM.CSV"],
    is_lookup_only=False,
    # TS lines 2947-2948: Hostname first, fallback Friendly Name
    key_col_aliases=["Hostname", "Friendly Name", "device_friendly_name"],
    header_strategy="fixed_0",
)

EDR = SourceDefinition(
    name="EDR",
    filename_candidates=["EDR.csv", "EDR.CSV"],
    is_lookup_only=False,
    # TS lines 2958-2959: Friendly Name first, fallback Hostname
    key_col_aliases=["Friendly Name", "Hostname"],
    header_strategy="fixed_0",
)

ASSET = SourceDefinition(
    name="ASSET",
    filename_candidates=["ASSET.CSV", "ASSET.csv"],
    is_lookup_only=True,
    # TS line 2878: "Nome do ativo" → normalizeAssetHostname
    key_col_aliases=["Nome do ativo", "Nome do ativo "],
    header_strategy="scan_keyword",
    scan_keyword="NOME DO ATIVO",
)

# Ordered join sequence; ASSET always last (lookup-only)
MACHINES_SOURCES: list[SourceDefinition] = [AD, UEM, EDR, ASSET]
MACHINES_SOURCES_BY_NAME: dict[str, SourceDefinition] = {s.name: s for s in MACHINES_SOURCES}
