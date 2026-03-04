from compliance_gate.domains.machines.classification.models import MachineRecord, MachineStatusDef, StatusSeverity
import re

STATUS_DEF = MachineStatusDef(
    key="PA_MISMATCH",
    label="🟠 DIVERGÊNCIA PA x USUÁRIO",
    severity=StatusSeverity.WARNING,
    description="Sufixo numérico da máquina (ex: 01) não bate com o sufixo numérico do usuário logado (ex: usuario_02).",
    is_flag=True
)

def extract_suffix(text: str) -> str:
    if not text:
        return ""
    # Look for trailing _XX
    match = re.search(r'_(\d{1,2})$', text.strip())
    if match:
        return match.group(1).zfill(2)
    return ""

def applies(record: MachineRecord, context: dict = None) -> bool:
    """
    DIVERGÊNCIA PA x USUÁRIO: Flag paralela comparando sufixos nome host x user logado.
    """
    machine_suffix = extract_suffix(record.hostname)
    # Prefer extra UEM logado or fallback to main user
    candidate_user = record.uem_extra_user_logado or record.main_user or ""
    
    if "\\" in candidate_user:
        candidate_user = candidate_user.split("\\")[-1]

    user_suffix = extract_suffix(candidate_user)

    if machine_suffix != "" and user_suffix != "" and machine_suffix != user_suffix:
        return True
    
    return False
