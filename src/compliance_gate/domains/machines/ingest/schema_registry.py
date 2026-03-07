"""
schema_registry.py — Canonical column names and aliases for Machines domain.

Centralizes the TS getIdx() alias logic in a testable, typed form.
All column matching is: upper() + strip() + BOM-strip — no fuzzy matching.

Based on: retests/ts_column_layout_reference.md and dashboard_fixed.ts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import polars as pl

_BOM = "\ufeff"


def normalize_col(raw: str) -> str:
    """Mirrors TS getIdx: upper + strip + BOM strip."""
    return raw.strip().lstrip(_BOM).upper()


# ─────────────────────────────────────────────────────────────────────────────
# Schema definitions (per source)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ColumnSpec:
    canonical: str              # the name we store internally
    aliases: list[str]          # all acceptable names from the CSV (including canonical)

    def matches(self, raw_col: str) -> bool:
        n = normalize_col(raw_col)
        return any(normalize_col(a) == n for a in self.aliases)


@dataclass(frozen=True)
class SourceSchema:
    source: str
    required: list[ColumnSpec]          # must be present (at least one alias)
    optional: list[ColumnSpec]          # logged if missing, not blocking

    def all_specs(self) -> list[ColumnSpec]:
        return self.required + self.optional


# ─────────────────────────────────────────────────────────────────────────────
# AD schema (TS lines 2941–2944)
# ─────────────────────────────────────────────────────────────────────────────

AD_SCHEMA = SourceSchema(
    source="AD",
    required=[
        ColumnSpec("computer_name", ["Computer Name", "ComputerName", "hostname"]),
    ],
    optional=[
        ColumnSpec("last_logon_time", ["Last Logon Time", "LastLogonTime"]),
        ColumnSpec("password_last_set", ["Password Last Set", "PasswordLastSet"]),
        ColumnSpec("operating_system", ["Operating System", "OperatingSystem", "OS"]),
    ],
)

# ─────────────────────────────────────────────────────────────────────────────
# UEM schema (TS lines 2947–2955)
# ─────────────────────────────────────────────────────────────────────────────

UEM_SCHEMA = SourceSchema(
    source="UEM",
    required=[
        # TS: Hostname first, fallback Friendly Name
        ColumnSpec("hostname", ["Hostname", "Friendly Name", "device_friendly_name"]),
    ],
    optional=[
        ColumnSpec("username", ["Username", "User Name"]),
        ColumnSpec("serial_number", ["Serial Number", "SerialNumber"]),
        ColumnSpec("last_seen", ["Last Seen", "DM Last Seen"]),
        ColumnSpec("os", ["OS", "Operating System"]),
        ColumnSpec("model", ["Model"]),
    ],
)

# ─────────────────────────────────────────────────────────────────────────────
# EDR schema (TS lines 2958–2971)
# ─────────────────────────────────────────────────────────────────────────────

EDR_SCHEMA = SourceSchema(
    source="EDR",
    required=[
        # TS: Friendly Name first, fallback Hostname
        ColumnSpec("hostname", ["Friendly Name", "Hostname"]),
    ],
    optional=[
        ColumnSpec("last_user", ["Last Logged In User Account", "Last User Account Login"]),
        ColumnSpec("last_seen", ["Last Seen"]),
        ColumnSpec("serial_number", ["Serial Number", "SerialNumber"]),
        ColumnSpec("os_version", ["OS Version"]),
        ColumnSpec("local_ip", ["Local IP"]),
        ColumnSpec("sensor_tags", ["Sensor Tags"]),
        ColumnSpec("chassis", ["Chassis"]),
    ],
)

# ─────────────────────────────────────────────────────────────────────────────
# ASSET schema (TS lines 2878–2880)
# ─────────────────────────────────────────────────────────────────────────────

ASSET_SCHEMA = SourceSchema(
    source="ASSET",
    required=[
        # Trailing space is intentional — some exports include it
        ColumnSpec("nome_do_ativo", ["Nome do ativo", "Nome do ativo "]),
    ],
    optional=[],
)

SCHEMAS: dict[str, SourceSchema] = {
    "AD": AD_SCHEMA,
    "UEM": UEM_SCHEMA,
    "EDR": EDR_SCHEMA,
    "ASSET": ASSET_SCHEMA,
}


# ─────────────────────────────────────────────────────────────────────────────
# Column resolver — replaces _col() in master_map_builder.py
# ─────────────────────────────────────────────────────────────────────────────

def resolve_col(
    df_columns: list[str],
    candidates: list[str],
) -> Optional[str]:
    """
    Find the first column name in df_columns that matches any candidate.
    Comparison: upper + strip + BOM strip (mirrors TS getIdx exactly).
    Returns the ORIGINAL column name as it appears in the DataFrame.
    """
    upper_map: dict[str, str] = {normalize_col(c): c for c in df_columns}
    for cand in candidates:
        found = upper_map.get(normalize_col(cand))
        if found is not None:
            return found
    return None


def resolve_spec(df_columns: list[str], spec: ColumnSpec) -> Optional[str]:
    """Resolve a ColumnSpec against actual DataFrame columns."""
    return resolve_col(df_columns, spec.aliases)


def validate_schema(
    df: pl.DataFrame,
    schema: SourceSchema,
) -> tuple[list[str], list[str]]:
    """
    Validate that a DataFrame has the required columns.

    Returns:
        (missing_required, missing_optional) — lists of canonical names not found.
    """
    cols = df.columns
    missing_required = [
        spec.canonical for spec in schema.required
        if resolve_spec(cols, spec) is None
    ]
    missing_optional = [
        spec.canonical for spec in schema.optional
        if resolve_spec(cols, spec) is None
    ]
    return missing_required, missing_optional
