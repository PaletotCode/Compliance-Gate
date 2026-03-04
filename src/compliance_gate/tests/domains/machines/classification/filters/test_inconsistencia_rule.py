import pytest
from compliance_gate.domains.machines.classification.models import MachineRecord
from compliance_gate.domains.machines.classification.filters.inconsistencia_de_base.rule import applies

def test_inconsistency_applies_when_missing_ad_but_has_uem():
    record = MachineRecord(hostname="test-01", pa_code="0001", has_ad=False, has_uem=True, has_edr=False)
    assert applies(record) is True

def test_inconsistency_applies_when_missing_ad_but_has_edr():
    record = MachineRecord(hostname="test-01", pa_code="0001", has_ad=False, has_uem=False, has_edr=True)
    assert applies(record) is True

def test_inconsistency_does_not_apply_when_has_ad():
    record = MachineRecord(hostname="test-01", pa_code="0001", has_ad=True, has_uem=True, has_edr=False)
    assert applies(record) is False

def test_inconsistency_does_not_apply_when_missing_all():
    # This should be PHANTOM
    record = MachineRecord(hostname="test-01", pa_code="0001", has_ad=False, has_uem=False, has_edr=False)
    assert applies(record) is False
