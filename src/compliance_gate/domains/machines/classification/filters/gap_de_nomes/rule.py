from compliance_gate.domains.machines.classification.models import MachineRecord, MachineStatusDef, StatusSeverity

STATUS_DEF = MachineStatusDef(
    key="GAP",
    label="🔴 GAP DE NOMES",
    severity=StatusSeverity.INFO,
    description="Sinaliza um 'buraco' na sequência numérica de Nomes de Máquinas de uma PA.",
    is_flag=False
)

def applies(record: MachineRecord, context: dict = None) -> bool:
    """
    GAP: Regra especial para hosts virtuais inseridos pela pipeline quando
    nota-se um salto nos numerais dos hostnames agrupados por PA.
    """
    return record.is_virtual_gap
