import pytest
from compliance_gate.domains.machines.classification.models import MachineRecord
from compliance_gate.domains.machines.classification.filters.perigo_sem_agente.rule import applies

def test_rogue_applies_when_has_ad_but_missing_agents():
    record = MachineRecord(hostname="test-01", pa_code="0001", has_ad=True, has_uem=False, has_edr=False)
    assert applies(record) is True

def test_rogue_does_not_apply_when_missing_ad():
    record = MachineRecord(hostname="test-01", pa_code="0001", has_ad=False, has_uem=False, has_edr=False)
    assert applies(record) is False

def test_rogue_does_not_apply_when_has_uem():
    record = MachineRecord(hostname="test-01", pa_code="0001", has_ad=True, has_uem=True, has_edr=False)
    assert applies(record) is False
