from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class SegmentPreviewResult(BaseModel):
    total_rows: int = Field(ge=0)
    matched_rows: int = Field(ge=0)
    match_rate: float = Field(ge=0.0, le=1.0)
    sample_rows: list[dict[str, Any]] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class ViewPreviewResult(BaseModel):
    total_rows: int = Field(ge=0)
    returned_rows: int = Field(ge=0)
    sample_rows: list[dict[str, Any]] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class ViewRunResult(BaseModel):
    total_rows: int = Field(ge=0)
    page: int = Field(ge=1)
    size: int = Field(ge=1)
    has_next: bool
    has_previous: bool
    columns: list[str] = Field(default_factory=list)
    items: list[dict[str, Any]] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)

