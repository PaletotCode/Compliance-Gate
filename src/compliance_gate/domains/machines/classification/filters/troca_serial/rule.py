from compliance_gate.domains.machines.classification.models import MachineRecord, MachineStatusDef, StatusSeverity

STATUS_DEF = MachineStatusDef(
    key="SWAP",
    label="🔄 TROCA DE SERIAL",
    severity=StatusSeverity.WARNING,
    description="Seriais UEM e EDR são diferentes para o mesmo hostname."
)

def applies(record: MachineRecord, context: dict = None) -> bool:
    """
    TROCA SERIAL: Seriais do UEM e EDR estão presentes, mas divergem.
    """
    if record.uem_serial and record.edr_serial:
        return record.uem_serial != record.edr_serial
    return False
