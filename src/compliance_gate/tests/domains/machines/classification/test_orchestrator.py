import pytest
from compliance_gate.domains.machines.classification.models import MachineRecord
from compliance_gate.domains.machines.classification.orchestrator import evaluate_machine

def test_orchestrator_inconsistency_has_highest_precedence():
    # Record that matches BOTH INCONSISTENCY and ROGUE or MISSING_UEM criteria
    # But since it has NO AD, it must be INCONSISTENCY (or PHANTOM)
    record = MachineRecord(hostname="test-01", pa_code="0001", has_ad=False, has_uem=True, has_edr=True)
    result = evaluate_machine(record)
    assert result.primary_status == "INCONSISTENCY"

def test_orchestrator_resolves_rogue():
    record = MachineRecord(hostname="test-01", pa_code="0001", has_ad=True, has_uem=False, has_edr=False)
    result = evaluate_machine(record)
    assert result.primary_status == "ROGUE"
    
def test_orchestrator_resolves_compliant_with_legacy_flag():
    record = MachineRecord(
        hostname="test-base", pa_code="0001",
        has_ad=True, has_uem=True, has_edr=True,
        uem_serial="abc", edr_serial="abc",
        ad_os="Windows 7 Professional"
    )
    result = evaluate_machine(record)
    assert result.primary_status == "COMPLIANT"
    assert "LEGACY" in result.flags

def test_orchestrator_resolves_gap_bypasses_all():
    record = MachineRecord(
        hostname="test-gap", pa_code="0001",
        is_virtual_gap=True, has_ad=True, has_uem=True, has_edr=True
    )
    result = evaluate_machine(record)
    assert result.primary_status == "GAP"
