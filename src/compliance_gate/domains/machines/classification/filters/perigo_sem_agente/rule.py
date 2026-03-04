from compliance_gate.domains.machines.classification.models import MachineRecord, MachineStatusDef, StatusSeverity

STATUS_DEF = MachineStatusDef(
    key="ROGUE",
    label="🚨 PERIGO (SEM AGENTE)",
    severity=StatusSeverity.DANGER,
    description="Tem AD, mas não tem nem UEM nem EDR."
)

def applies(record: MachineRecord, context: dict = None) -> bool:
    """
    PERIGO (SEM AGENTE): AD && !UEM && !EDR
    """
    return record.has_ad and not record.has_uem and not record.has_edr
