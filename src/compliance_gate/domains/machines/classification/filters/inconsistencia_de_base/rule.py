from compliance_gate.domains.machines.classification.models import MachineRecord, MachineStatusDef, StatusSeverity

STATUS_DEF = MachineStatusDef(
    key="INCONSISTENCY",
    label="🧩 INCONSISTÊNCIA DE BASE",
    severity=StatusSeverity.DANGER,
    description="Falta no AD, mas existe no UEM ou EDR."
)

def applies(record: MachineRecord, context: dict = None) -> bool:
    """
    INCONSISTÊNCIA DE BASE: !AD && (UEM || EDR)
    """
    return not record.has_ad and (record.has_uem or record.has_edr)
