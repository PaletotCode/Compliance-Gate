from __future__ import annotations

from collections.abc import Mapping, Set

from fastapi import HTTPException

from compliance_gate.Engine.errors import DeclarativeEngineError, GuardrailViolation


def declarative_error_status(
    exc: DeclarativeEngineError,
    *,
    not_found_reasons: Set[str] | None = None,
    conflict_reasons: Set[str] | None = None,
    gone_reasons: Set[str] | None = None,
    explicit_reason_status: Mapping[str, int] | None = None,
    default_status: int = 400,
    guardrail_status: int | None = None,
) -> int:
    reason = exc.details.get("reason")

    if isinstance(reason, str):
        if reason.endswith("_not_found"):
            return 404
        if not_found_reasons and reason in not_found_reasons:
            return 404
        if conflict_reasons and reason in conflict_reasons:
            return 409
        if gone_reasons and reason in gone_reasons:
            return 410
        if explicit_reason_status and reason in explicit_reason_status:
            return explicit_reason_status[reason]

    if guardrail_status is not None and isinstance(exc, GuardrailViolation):
        return guardrail_status

    return default_status


def raise_declarative_http(
    exc: DeclarativeEngineError,
    *,
    not_found_reasons: Set[str] | None = None,
    conflict_reasons: Set[str] | None = None,
    gone_reasons: Set[str] | None = None,
    explicit_reason_status: Mapping[str, int] | None = None,
    default_status: int = 400,
    guardrail_status: int | None = None,
) -> None:
    raise HTTPException(
        status_code=declarative_error_status(
            exc,
            not_found_reasons=not_found_reasons,
            conflict_reasons=conflict_reasons,
            gone_reasons=gone_reasons,
            explicit_reason_status=explicit_reason_status,
            default_status=default_status,
            guardrail_status=guardrail_status,
        ),
        detail=exc.to_dict(),
    ) from exc
