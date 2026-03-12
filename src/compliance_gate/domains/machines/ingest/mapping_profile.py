"""
mapping_profile.py — Data structures for CSV Tab Configs (Profiles).

Defines the Pydantic schemas that represent a parsing configuration profile.
Replaces the placeholder from Chat 1.
"""

from __future__ import annotations

from typing import Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

ProfileScope = Literal["PRIVATE", "TEAM", "TENANT", "GLOBAL"]


class CsvTabConfig(BaseModel):
    """
    Detailed parsing configuration for a single CSV source.
    Saved as JSON inside CsvTabProfileVersion.payload_json.
    """

    model_config = ConfigDict(extra="ignore")

    header_row: int = Field(
        default=0,
        ge=0,
        validation_alias=AliasChoices("header_row", "header_row_index"),
        description="0-based index of the header row",
    )
    delimiter: str | None = Field(
        default=None, description="Force a specific delimiter, e.g. ';'. Empty triggers Sniffer"
    )
    encoding: str | None = Field(
        default=None,
        description="Force a specific encoding, e.g. 'utf-8-sig'. Empty allows fallback chain",
    )
    sic_column: str = Field(
        ..., description="Canonical key to join on (exact header name in the CSV)"
    )
    selected_columns: list[str] = Field(
        default_factory=list, description="List of raw headers to show in the UI table"
    )
    alias_map: dict[str, str] = Field(
        default_factory=dict, description="Map of {csv_header: canonical_column} (futures)"
    )
    normalize_key_strategy: str = Field(
        default="ts_default", description="Strategy to clean keys (upper/strip/bom)"
    )


class CsvTabDraftConfig(BaseModel):
    """
    Partial CSV tab config used for autosave drafts in upload sessions.
    Allows incomplete state while the user is still editing.
    """

    model_config = ConfigDict(extra="ignore")

    header_row: int | None = Field(
        default=None,
        ge=0,
        validation_alias=AliasChoices("header_row", "header_row_index"),
        description="0-based index of the header row",
    )
    delimiter: str | None = Field(default=None)
    encoding: str | None = Field(default=None)
    sic_column: str | None = Field(default=None)
    selected_columns: list[str] = Field(default_factory=list)
    alias_map: dict[str, str] = Field(default_factory=dict)
    normalize_key_strategy: str | None = Field(default=None)

    def is_complete(self) -> bool:
        return bool(self.sic_column and self.sic_column.strip())

    def to_complete_config(self, fallback: CsvTabConfig | None = None) -> CsvTabConfig | None:
        sic = (self.sic_column or "").strip() or (fallback.sic_column if fallback else "")
        if not sic:
            return None
        return CsvTabConfig(
            header_row=self.header_row if self.header_row is not None else (fallback.header_row if fallback else 0),
            delimiter=self.delimiter if self.delimiter is not None else (fallback.delimiter if fallback else None),
            encoding=self.encoding if self.encoding is not None else (fallback.encoding if fallback else None),
            sic_column=sic,
            selected_columns=self.selected_columns or (fallback.selected_columns if fallback else []),
            alias_map=self.alias_map or (fallback.alias_map if fallback else {}),
            normalize_key_strategy=(
                self.normalize_key_strategy
                if self.normalize_key_strategy is not None
                else (fallback.normalize_key_strategy if fallback else "ts_default")
            ),
        )

    @staticmethod
    def from_complete(config: CsvTabConfig) -> "CsvTabDraftConfig":
        return CsvTabDraftConfig(
            header_row=config.header_row,
            delimiter=config.delimiter,
            encoding=config.encoding,
            sic_column=config.sic_column,
            selected_columns=list(config.selected_columns),
            alias_map=dict(config.alias_map),
            normalize_key_strategy=config.normalize_key_strategy,
        )


class CsvTabProfileSchema(BaseModel):
    """
    API representation of a CsvTabProfile.
    """

    id: str
    tenant_id: str
    source: str
    scope: str
    owner_user_id: str | None
    name: str
    active_version: int
    is_default_for_source: bool
    payload: CsvTabConfig | None = None  # Populated when fetching by ID with details

    model_config = ConfigDict(from_attributes=True)
