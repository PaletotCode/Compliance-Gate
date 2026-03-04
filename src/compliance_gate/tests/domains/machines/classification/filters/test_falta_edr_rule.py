import pytest
from compliance_gate.domains.machines.classification.models import MachineRecord
from compliance_gate.domains.machines.classification.filters.falta_edr.rule import applies

def test_missing_edr_applies():
    record = MachineRecord(hostname="test-01", pa_code="0001", has_ad=True, has_uem=True, has_edr=False)
    assert applies(record) is True

def test_missing_edr_does_not_apply():
    record = MachineRecord(hostname="test-01", pa_code="0001", has_ad=True, has_uem=True, has_edr=True)
    assert applies(record) is False
