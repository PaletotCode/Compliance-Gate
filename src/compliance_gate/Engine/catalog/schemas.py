from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class CatalogColumnProfile(BaseModel):
    name: str
    data_type: str
    sample_values: list[Any] = Field(default_factory=list)
    null_rate: float = Field(ge=0.0, le=1.0)
    approx_cardinality: int = Field(ge=0)


class MachinesFinalCatalogSnapshot(BaseModel):
    tenant_id: str
    dataset_version_id: str
    row_count: int = Field(ge=0)
    columns: list[CatalogColumnProfile]

