from __future__ import annotations

from enum import StrEnum


class EngineSource(StrEnum):
    AD = "AD"
    UEM = "UEM"
    EDR = "EDR"
    ASSET = "ASSET"


ACTIVE_SOURCES: tuple[EngineSource, ...] = (
    EngineSource.AD,
    EngineSource.UEM,
    EngineSource.EDR,
    EngineSource.ASSET,
)
