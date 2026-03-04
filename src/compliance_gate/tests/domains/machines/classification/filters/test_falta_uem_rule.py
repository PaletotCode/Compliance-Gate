import pytest
from compliance_gate.domains.machines.classification.models import MachineRecord
from compliance_gate.domains.machines.classification.filters.falta_uem.rule import applies

def test_missing_uem_applies():
    record = MachineRecord(hostname="test-01", pa_code="0001", has_ad=True, has_uem=False, has_edr=True)
    assert applies(record) is True

def test_missing_uem_does_not_apply():
    record = MachineRecord(hostname="test-01", pa_code="0001", has_ad=True, has_uem=True, has_edr=True)
    assert applies(record) is False
