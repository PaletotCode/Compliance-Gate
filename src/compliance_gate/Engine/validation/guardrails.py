from __future__ import annotations

from pydantic import ValidationError

from compliance_gate.Engine.reports.definitions import (
    ReportExecutionPlan,
    ReportRequest,
    load_template,
    resolve_effective_limit,
)


class EngineGuardrailException(Exception):
    pass


def validate_report_request(payload: dict) -> tuple[ReportRequest, ReportExecutionPlan]:
    try:
        request = ReportRequest(**payload)
    except ValidationError as exc:
        raise EngineGuardrailException(f"invalid request payload: {exc}") from exc

    try:
        template = load_template(request.template_name)
    except Exception as exc:
        raise EngineGuardrailException(str(exc)) from exc

    effective_limit = resolve_effective_limit(template, request.limit)
    if effective_limit <= 0:
        raise EngineGuardrailException("invalid limit")

    # Query is rendered later by runner; plan carries safe bounds only.
    plan = ReportExecutionPlan(
        template_name=request.template_name,
        query="",
        effective_limit=effective_limit,
    )
    return request, plan
