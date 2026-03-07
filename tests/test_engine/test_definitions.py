"""
test_definitions.py - Tests Engine Core schemas and definition guardrails.
"""
import pytest
from compliance_gate.Engine.reports.definitions import ReportDefinitionSchema
from compliance_gate.Engine.validation.guardrails import validate_schema_safety, EngineGuardrailException
from compliance_gate.Engine.config.engine_settings import engine_settings

def test_valid_schema():
    payload = {
        "version": 1,
        "base_spine": "machines_final",
        "target_columns": [{"name": "hostname", "expression": "hostname"}],
        "limit": 100
    }
    schema = validate_schema_safety(payload)
    assert schema.base_spine == "machines_final"
    assert len(schema.target_columns) == 1

def test_limit_exceeded():
    payload = {
        "version": 1,
        "base_spine": "machines_final",
        "target_columns": [{"name": "hostname", "expression": "hostname"}],
        "limit": engine_settings.max_report_rows + 1
    }
    with pytest.raises(EngineGuardrailException, match=r"(?s)Invalid report schema.*less_than_equal"):
        validate_schema_safety(payload)

def test_sql_injection_guard():
    # Attempting to inject a comment
    payload = {
        "version": 1,
        "base_spine": "machines_final",
        "target_columns": [{"name": "hostname", "expression": "hostname; DROP TABLE something; --"}],
        "limit": 100
    }
    with pytest.raises(EngineGuardrailException, match="Unsafe expression"):
        validate_schema_safety(payload)

def test_group_by_injection_guard():
    payload = {
        "version": 1,
        "base_spine": "machines_final",
        "target_columns": [{"name": "status", "expression": "status"}],
        "group_by": ["status; -- injection"],
        "limit": 100
    }
    with pytest.raises(EngineGuardrailException, match="Unsafe group_by"):
        validate_schema_safety(payload)
