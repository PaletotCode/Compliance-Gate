import time
from compliance_gate.domains.machines.classification.models import MachineRecord, MachineStatusDef, StatusSeverity

STATUS_DEF = MachineStatusDef(
    key="OFFLINE",
    label="💤 OFFLINE",
    severity=StatusSeverity.WARNING,
    description="Ativo seguro mas sem sinalização de atividade recente na rede."
)

def applies(record: MachineRecord, context: dict = None) -> bool:
    """
    OFFLINE: Último sinal > staleDays.
    Esta regra entra após as regras graves (como falta edr ou rogue) 
    para garantir que a máquina só é offline se estivesse 'Segura'.
    """
    if record.last_seen_date_ms and record.last_seen_date_ms > 0:
        now_ms = int(time.time() * 1000)
        stale_days = context.get("stale_days", 45) if context else 45
        
        diff_ms = now_ms - record.last_seen_date_ms
        days = diff_ms / (1000 * 60 * 60 * 24)
        return days > stale_days
        
    return False
