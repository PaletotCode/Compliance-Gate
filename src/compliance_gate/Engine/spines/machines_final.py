from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from compliance_gate.Engine.spines.models import SpineDefinition

MACHINES_FINAL_SPINE = SpineDefinition(domain="machines", name="machines_final")


class MachinesFinalRow(BaseModel):
    machine_id: str
    hostname: str
    pa_code: str
    primary_status: str
    primary_status_label: str
    flags: list[str]
    has_ad: bool
    has_uem: bool
    has_edr: bool
    has_asset: bool
    last_seen_date_ms: int | None = None

    model_config = ConfigDict(extra="ignore")


MACHINES_FINAL_COLUMNS: tuple[str, ...] = (
    "machine_id",
    "hostname",
    "pa_code",
    "primary_status",
    "primary_status_label",
    "flags",
    "has_ad",
    "has_uem",
    "has_edr",
    "has_asset",
    "last_seen_date_ms",
)
