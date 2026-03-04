import pytest
from compliance_gate.domains.machines.classification.models import MachineRecord
from compliance_gate.domains.machines.classification.filters.fantasma_ad.rule import applies

def test_phantom_applies_when_missing_all_primary_sources():
    record = MachineRecord(hostname="test-01", pa_code="0001", has_ad=False, has_uem=False, has_edr=False)
    assert applies(record) is True

def test_phantom_does_not_apply_when_has_ad():
    record = MachineRecord(hostname="test-01", pa_code="0001", has_ad=True, has_uem=False, has_edr=False)
    assert applies(record) is False

def test_phantom_does_not_apply_when_has_uem():
    record = MachineRecord(hostname="test-01", pa_code="0001", has_ad=False, has_uem=True, has_edr=False)
    assert applies(record) is False
