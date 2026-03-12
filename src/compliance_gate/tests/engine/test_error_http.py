from __future__ import annotations

from compliance_gate.Engine.errors import GuardrailViolation, InvalidExpressionSyntax
from compliance_gate.Engine.interfaces.error_http import declarative_error_status


def test_declarative_error_status_defaults_not_found_suffix() -> None:
    exc = InvalidExpressionSyntax(
        details={"reason": "segment_not_found"},
    )
    assert declarative_error_status(exc) == 404


def test_declarative_error_status_uses_custom_reason_mappings() -> None:
    exc = InvalidExpressionSyntax(details={"reason": "unique_violation"})
    assert declarative_error_status(exc, conflict_reasons={"unique_violation"}) == 409


def test_declarative_error_status_supports_guardrail_override() -> None:
    exc = GuardrailViolation(details={"reason": "row_limit_exceeded"})
    assert declarative_error_status(exc, guardrail_status=422) == 422
