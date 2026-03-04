from compliance_gate.domains.machines.classification.models import MachineRecord, MachineStatusDef, StatusSeverity

STATUS_DEF = MachineStatusDef(
    key="MISSING_EDR",
    label="⚠️ FALTA EDR",
    severity=StatusSeverity.WARNING,
    description="Tem AD e UEM, mas falta EDR."
)

def applies(record: MachineRecord, context: dict = None) -> bool:
    """
    FALTA EDR: AD && UEM && !EDR
    """
    return record.has_ad and record.has_uem and not record.has_edr
