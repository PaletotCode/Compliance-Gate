import pytest
from compliance_gate.domains.machines.classification.models import MachineRecord
from compliance_gate.domains.machines.classification.filters.troca_serial.rule import applies

def test_swap_applies_when_serials_differ():
    record = MachineRecord(
        hostname="test-01", pa_code="0001", 
        has_ad=True, has_uem=True, has_edr=True,
        uem_serial="123", edr_serial="456"
    )
    assert applies(record) is True

def test_swap_does_not_apply_when_serials_match():
    record = MachineRecord(
        hostname="test-01", pa_code="0001", 
        has_ad=True, has_uem=True, has_edr=True,
        uem_serial="123", edr_serial="123"
    )
    assert applies(record) is False

def test_swap_does_not_apply_when_missing_serial():
    record = MachineRecord(
        hostname="test-01", pa_code="0001", 
        has_ad=True, has_uem=True, has_edr=True,
        uem_serial=None, edr_serial="123"
    )
    assert applies(record) is False
