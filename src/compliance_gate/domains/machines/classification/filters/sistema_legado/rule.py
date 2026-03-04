from compliance_gate.domains.machines.classification.models import MachineRecord, MachineStatusDef, StatusSeverity

STATUS_DEF = MachineStatusDef(
    key="LEGACY",
    label="🧓 SISTEMA LEGADO",
    severity=StatusSeverity.WARNING,
    description="Sistema Operacional corresponde a uma string legada (ex: Win 7).",
    is_flag=True
)

def applies(record: MachineRecord, context: dict = None) -> bool:
    """
    SISTEMA LEGADO: Flag (paralela) se OS do AD string contém legacy definition.
    """
    if not record.ad_os:
        return False
        
    # Example config injection, normally comes from DB or settings
    legacy_defs = context.get("legacy_definitions", ["Windows 7", "Windows 8", "Windows XP", "Windows Server 2008", "Windows Server 2012"]) if context else []
    
    os_upper = record.ad_os.upper()
    for l_def in legacy_defs:
        if l_def.upper() in os_upper:
            return True
            
    return False
