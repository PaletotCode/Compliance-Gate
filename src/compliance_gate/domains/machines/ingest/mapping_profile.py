"""
mapping_profile.py — Data structures for CSV Tab Configs (Profiles).

Defines the Pydantic schemas that represent a parsing configuration profile.
Replaces the placeholder from Chat 1.
"""

from __future__ import annotations

from typing import Literal, Optional
from pydantic import BaseModel, ConfigDict, Field


ProfileScope = Literal["PRIVATE", "TEAM", "TENANT", "GLOBAL"]


class CsvTabConfig(BaseModel):
    """
    Detailed parsing configuration for a single CSV source.
    Saved as JSON inside CsvTabProfileVersion.payload_json.
    """
    model_config = ConfigDict(extra="ignore")

    header_row: int = Field(default=0, ge=0, description="0-based index of the header row")
    delimiter: Optional[str] = Field(default=None, description="Force a specific delimiter, e.g. ';'. Empty triggers Sniffer")
    encoding: Optional[str] = Field(default=None, description="Force a specific encoding, e.g. 'utf-8-sig'. Empty allows fallback chain")
    sic_column: str = Field(..., description="Canonical key to join on (exact header name in the CSV)")
    selected_columns: list[str] = Field(default_factory=list, description="List of raw headers to show in the UI table")
    alias_map: dict[str, str] = Field(default_factory=dict, description="Map of {csv_header: canonical_column} (futures)")
    normalize_key_strategy: str = Field(default="ts_default", description="Strategy to clean keys (upper/strip/bom)")


class CsvTabProfileSchema(BaseModel):
    """
    API representation of a CsvTabProfile.
    """
    id: str
    tenant_id: str
    source: str
    scope: str
    owner_user_id: Optional[str]
    name: str
    active_version: int
    is_default_for_source: bool
    payload: Optional[CsvTabConfig] = None  # Populated when fetching by ID with details

    model_config = ConfigDict(from_attributes=True)
