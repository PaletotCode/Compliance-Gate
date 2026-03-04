from compliance_gate.domains.machines.classification.models import MachineRecord, MachineStatusDef, StatusSeverity

STATUS_DEF = MachineStatusDef(
    key="MISSING_ASSET",
    label="📦 FALTA ASSET",
    severity=StatusSeverity.WARNING,
    description="Máquina no AD e na rede (UEM/EDR), mas falta cadastro em ASSET.",
    is_flag=True
)

def applies(record: MachineRecord, context: dict = None) -> bool:
    """
    FALTA ASSET: Flag paralela (AD && !ASSET && (UEM || EDR))
    """
    return record.has_ad and not record.has_asset and (record.has_uem or record.has_edr)
