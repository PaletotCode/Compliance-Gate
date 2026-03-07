"""
asset_person_sic.py - Bridge definition matching an Asset/Device to a Person (SIC).
"""

from .base import BaseBridge, EngineBridgeDef

asset_person_bridge_def = EngineBridgeDef(
    name="asset_to_person",
    description="Connects an endpoint Machine to its assigned Person via SIC correlation.",
    source_spine="machines_final",
    target_spines=["person_dim"],
    join_keys=["assigned_user_cpf"],
)


class AssetPersonBridge(BaseBridge):
    definition = asset_person_bridge_def
