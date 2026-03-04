from compliance_gate.domains.machines.classification.models import MachineRecord, MachineStatusDef, StatusSeverity

STATUS_DEF = MachineStatusDef(
    key="PHANTOM",
    label="👻 FANTASMA (AD)",
    severity=StatusSeverity.WARNING,
    description="Falta no AD e não caiu em inconsistência. Só existe no ASSET."
)

def applies(record: MachineRecord, context: dict = None) -> bool:
    """
    FANTASMA (AD): !AD && !UEM && !EDR
    """
    return not record.has_ad and not record.has_uem and not record.has_edr
