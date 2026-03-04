from compliance_gate.domains.machines.classification.models import MachineRecord, MachineStatusDef, StatusSeverity

STATUS_DEF = MachineStatusDef(
    key="COMPLIANT",
    label="✅ SEGURO (OK)",
    severity=StatusSeverity.SUCCESS,
    description="Máquina aderente às políticas (Agentes rodando e seriais corretos)."
)

def applies(record: MachineRecord, context: dict = None) -> bool:
    """
    SEGURO: Fallback default na cadeia de precedência principal, 
    se não cair em nenhum erro, ela é segura por padrão.
    """
    return True
