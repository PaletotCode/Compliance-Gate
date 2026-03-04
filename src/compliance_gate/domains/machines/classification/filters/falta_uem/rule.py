from compliance_gate.domains.machines.classification.models import MachineRecord, MachineStatusDef, StatusSeverity

STATUS_DEF = MachineStatusDef(
    key="MISSING_UEM",
    label="⚠️ FALTA UEM",
    severity=StatusSeverity.WARNING,
    description="Tem AD e EDR, mas falta UEM."
)

def applies(record: MachineRecord, context: dict = None) -> bool:
    """
    FALTA UEM: AD && !UEM && EDR
    """
    return record.has_ad and not record.has_uem and record.has_edr
