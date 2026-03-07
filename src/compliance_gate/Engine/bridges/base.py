"""
base.py - Base abstractions for Engine Bridges.
Bridges represent approved pathways to JOIN different Data Spines.
"""
from pydantic import BaseModel

from compliance_gate.Engine.spines.models import SpineTable


class EngineBridgeDef(BaseModel):
    """
    Defines a logical connection between two or more Spines.
    """
    name: str
    description: str
    source_spine: str
    target_spines: list[str]
    join_keys: list[str]

class BaseBridge:
    """
    Interface for bridge execution.
    """
    definition: EngineBridgeDef

    def validate_join(self, left: SpineTable, right: SpineTable) -> bool:
        """
        Future proof validation: assert if a join is actually permissible given current schemas.
        """
        return True
