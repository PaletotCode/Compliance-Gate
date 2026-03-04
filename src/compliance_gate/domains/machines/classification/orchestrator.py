import importlib
from typing import List

from .models import MachineRecord, MachineStatusResult

# Enumerate filter modules in precedence order for Primary Status
PRIMARY_FILTERS_ORDER = [
    "inconsistencia_de_base",
    "fantasma_ad",
    "perigo_sem_agente",
    "falta_uem",
    "falta_edr",
    "troca_serial",
    "duplicado",
    "offline",
    "seguro_ok",
]

# Enumerate parallel flag filters
FLAG_FILTERS = [
    "sistema_legado",
    "falta_asset",
    "divergencia_pa_x_usuario",
]

# Enumerate special cases (mutually exclusive with the rest of the flow usually)
# GAPs and DISPONIVEL might bypass standard AD/UEM checks
SPECIAL_FILTERS = [
    "gap_de_nomes",
    "disponivel",
]

def load_rule(module_name: str):
    """Dynamically loads the rule module to prevent massive imports in orchestrator."""
    module_path = f"compliance_gate.domains.machines.classification.filters.{module_name}.rule"
    return importlib.import_module(module_path)

def evaluate_machine(record: MachineRecord, stale_days_config: int = 30) -> MachineStatusResult:
    """
    Evaluates a single MachineRecord through the chain of isolated filters.
    Returns the MachineStatusResult with exactly 1 primary status and N flags.
    """
    
    # 1. Evaluate Special Cases First (Bypass everything else if hit)
    gap_rule = load_rule("gap_de_nomes")
    if gap_rule.applies(record):
        return MachineStatusResult(
            primary_status=gap_rule.STATUS_DEF.key,
            primary_status_label=gap_rule.STATUS_DEF.label,
            flags=[]
        )

    disp_rule = load_rule("disponivel")
    if disp_rule.applies(record):
        return MachineStatusResult(
            primary_status=disp_rule.STATUS_DEF.key,
            primary_status_label=disp_rule.STATUS_DEF.label,
            flags=[]
        )

    # 2. Evaluate Primary Status (First Match Wins)
    primary_status_key = "UNKNOWN"
    primary_status_label = "Desconhecido"
    
    context = {"stale_days": stale_days_config}

    for module_name in PRIMARY_FILTERS_ORDER:
        rule = load_rule(module_name)
        if rule.applies(record, context):
            primary_status_key = rule.STATUS_DEF.key
            primary_status_label = rule.STATUS_DEF.label
            break

    # fallback just in case
    if primary_status_key == "UNKNOWN":
        ok_rule = load_rule("seguro_ok")
        primary_status_key = ok_rule.STATUS_DEF.key
        primary_status_label = ok_rule.STATUS_DEF.label

    # 3. Evaluate Flags (Parallel, additive)
    flags = []
    for module_name in FLAG_FILTERS:
        rule = load_rule(module_name)
        if rule.applies(record, context):
            flags.append(rule.STATUS_DEF.key)

    return MachineStatusResult(
        primary_status=primary_status_key,
        primary_status_label=primary_status_label,
        flags=flags
    )
