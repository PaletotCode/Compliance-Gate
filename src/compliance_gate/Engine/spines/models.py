from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class EngineBaseMetrics:
    elapsed_ms: float = 0.0
    row_count: int = 0
    checksum: str = ""


@dataclass(slots=True)
class SpineDefinition:
    domain: str
    name: str
