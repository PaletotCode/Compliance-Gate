from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from compliance_gate.Engine.config.engine_settings import engine_settings

TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"


class ReportTemplate(BaseModel):
    template_name: str
    description: str
    base_spine: str
    kind: Literal["machines_status_summary"]
    default_limit: int = Field(default=1000, ge=1)
    max_limit: int = Field(default=engine_settings.max_report_rows, ge=1)


class ReportRequest(BaseModel):
    template_name: str
    limit: int | None = Field(default=None, ge=1)


class ReportExecutionPlan(BaseModel):
    template_name: str
    query: str
    effective_limit: int


@lru_cache(maxsize=32)
def load_template(template_name: str) -> ReportTemplate:
    template_path = TEMPLATE_DIR / f"{template_name}.json"
    if not template_path.exists():
        raise ValueError(f"template not found: {template_name}")

    with template_path.open("r", encoding="utf-8") as file_obj:
        payload = json.load(file_obj)
    return ReportTemplate(**payload)


def resolve_effective_limit(template: ReportTemplate, requested_limit: int | None) -> int:
    raw_limit = requested_limit if requested_limit is not None else template.default_limit
    return min(raw_limit, template.max_limit, engine_settings.max_report_rows)


def list_templates() -> list[str]:
    return sorted(path.stem for path in TEMPLATE_DIR.glob("*.json"))
