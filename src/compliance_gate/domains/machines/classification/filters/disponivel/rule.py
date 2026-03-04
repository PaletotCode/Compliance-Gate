from compliance_gate.domains.machines.classification.models import MachineRecord, MachineStatusDef, StatusSeverity

STATUS_DEF = MachineStatusDef(
    key="AVAILABLE",
    label="ℹ️ DISPONÍVEL",
    severity=StatusSeverity.INFO,
    description="Máquina que consta como disponível em estoque.",
    is_flag=False
)

def applies(record: MachineRecord, context: dict = None) -> bool:
    """
    DISPONÍVEL: Regra especial, separada de GAP, para as máquinas que têm
    a flag `is_available_in_asset` setada verdadeira via ingestão.
    """
    return record.is_available_in_asset
