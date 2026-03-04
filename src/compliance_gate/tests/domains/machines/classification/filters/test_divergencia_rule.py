import pytest
from compliance_gate.domains.machines.classification.models import MachineRecord
from compliance_gate.domains.machines.classification.filters.divergencia_pa_x_usuario.rule import applies

def test_pa_mismatch_applies_when_suffixes_differ():
    record = MachineRecord(hostname="HOST_01", pa_code="0001", main_user="user_02")
    assert applies(record) is True

def test_pa_mismatch_does_not_apply_when_suffixes_match():
    record = MachineRecord(hostname="HOST_01", pa_code="0001", main_user="user_01")
    assert applies(record) is False

def test_pa_mismatch_extracts_from_domain_user():
    record = MachineRecord(hostname="HOST_03", pa_code="0001", main_user="DOMAIN\\user_04")
    assert applies(record) is True
    
def test_pa_mismatch_prefers_uem_extra_user():
    record = MachineRecord(hostname="HOST_01", pa_code="0001", main_user="user_01", uem_extra_user_logado="user_02")
    assert applies(record) is True
