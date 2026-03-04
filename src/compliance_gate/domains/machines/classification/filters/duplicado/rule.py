from compliance_gate.domains.machines.classification.models import MachineRecord, MachineStatusDef, StatusSeverity

STATUS_DEF = MachineStatusDef(
    key="CLONE",
    label="👯 DUPLICADO",
    severity=StatusSeverity.WARNING,
    description="Mesmo Serial do EDR associado a múltiplos Hostnames distintos."
)

def applies(record: MachineRecord, context: dict = None) -> bool:
    """
    DUPLICADO (CLONE): Assinalado pelo estágio de pre-processamento de serial map.
    """
    return record.serial_is_cloned
