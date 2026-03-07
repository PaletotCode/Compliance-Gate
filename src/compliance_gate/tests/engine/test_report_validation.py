from __future__ import annotations

import pytest

from compliance_gate.Engine.config.engine_settings import engine_settings
from compliance_gate.Engine.reports.definitions import load_template
from compliance_gate.Engine.validation.guardrails import (
    EngineGuardrailException,
    validate_report_request,
)


def test_load_machines_status_summary_template() -> None:
    template = load_template("machines_status_summary")
    assert template.template_name == "machines_status_summary"
    assert template.kind == "machines_status_summary"


def test_validate_report_request_limit_is_capped() -> None:
    _, plan = validate_report_request(
        {
            "template_name": "machines_status_summary",
            "limit": engine_settings.max_report_rows + 999,
        }
    )
    assert plan.effective_limit == engine_settings.max_report_rows


def test_validate_report_request_invalid_template() -> None:
    with pytest.raises(EngineGuardrailException):
        validate_report_request({"template_name": "does_not_exist"})
