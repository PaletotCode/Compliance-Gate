from __future__ import annotations

from compliance_gate.domains.machines.classification.models import MachineStatusResult
from compliance_gate.Engine.config.engine_settings import engine_settings
from compliance_gate.Engine.rulesets import (
    ClassificationRuntimeMode,
    RuleSetPayloadV2,
    compile_ruleset_from_payload,
    dry_run_ruleset,
    explain_row,
    validate_ruleset_payload,
)


def _row(
    *,
    hostname: str = "HOST-01",
    pa_code: str = "PA-1",
    has_edr: bool = True,
    has_ad: bool = True,
) -> dict:
    return {
        "hostname": hostname,
        "pa_code": pa_code,
        "has_edr": has_edr,
        "has_ad": has_ad,
    }


def test_validate_ruleset_payload_reports_unknown_column_with_suggestions() -> None:
    payload = RuleSetPayloadV2(
        blocks=[
            {
                "kind": "primary",
                "entries": [
                    {
                        "rule_key": "bad_column",
                        "priority": 1,
                        "condition": {
                            "node_type": "binary_op",
                            "operator": "==",
                            "left": {"node_type": "column_ref", "column": "hostnme"},
                            "right": {"node_type": "literal", "value_type": "string", "value": "x"},
                        },
                        "output": {"primary_status": "ROGUE"},
                    }
                ],
            }
        ]
    )
    result = validate_ruleset_payload(payload, column_types={"hostname": "string"})
    assert result["is_valid"] is False
    assert result["summary"]["error_count"] == 1
    issue = result["issues"][0]
    assert issue["code"] == "UnknownColumn"
    assert issue["stage"] == "semantics"
    assert issue["node_path"] == "root.left"
    assert "hostname" in issue["details"]["suggestions"]


def test_validate_ruleset_payload_reports_rule_output_conflict() -> None:
    payload = RuleSetPayloadV2(
        blocks=[
            {
                "kind": "primary",
                "entries": [
                    {
                        "rule_key": "conflict_status",
                        "priority": 1,
                        "condition": {
                            "node_type": "literal",
                            "value_type": "bool",
                            "value": True,
                        },
                        "output": {"primary_status": "A", "status": "B"},
                    }
                ],
            }
        ]
    )
    result = validate_ruleset_payload(payload)
    assert result["is_valid"] is False
    assert result["summary"]["error_count"] == 1
    issue = result["issues"][0]
    assert issue["code"] == "RuleOutputConflict"
    assert issue["stage"] == "viability"
    assert issue["node_path"] == "blocks[0].entries[0].output"


def test_validate_ruleset_payload_reports_unreachable_warning() -> None:
    payload = RuleSetPayloadV2(
        blocks=[
            {
                "kind": "primary",
                "entries": [
                    {
                        "rule_key": "always_true",
                        "priority": 1,
                        "condition": {
                            "node_type": "literal",
                            "value_type": "bool",
                            "value": True,
                        },
                        "output": {"primary_status": "ALWAYS"},
                    },
                    {
                        "rule_key": "never_reached",
                        "priority": 2,
                        "condition": {
                            "node_type": "function_call",
                            "function_name": "is_not_null",
                            "arguments": [{"node_type": "column_ref", "column": "hostname"}],
                        },
                        "output": {"primary_status": "NEVER"},
                    },
                ],
            }
        ]
    )
    result = validate_ruleset_payload(payload)
    assert result["is_valid"] is True
    assert result["summary"]["warning_count"] == 1
    warning = result["warnings"][0]
    assert warning["code"] == "UnreachableRuleWarning"
    assert warning["stage"] == "viability"
    assert warning["node_path"] == "blocks[0].entries[1].condition"


def test_explain_row_returns_failed_conditions_and_decision_reason() -> None:
    payload = RuleSetPayloadV2(
        blocks=[
            {
                "kind": "primary",
                "entries": [
                    {
                        "rule_key": "missing_edr",
                        "priority": 1,
                        "condition": {
                            "node_type": "binary_op",
                            "operator": "==",
                            "left": {"node_type": "column_ref", "column": "has_edr"},
                            "right": {"node_type": "literal", "value_type": "bool", "value": False},
                        },
                        "output": {"primary_status": "MISSING_EDR"},
                    }
                ],
            },
            {
                "kind": "flags",
                "entries": [
                    {
                        "rule_key": "no_ad",
                        "priority": 10,
                        "condition": {
                            "node_type": "binary_op",
                            "operator": "==",
                            "left": {"node_type": "column_ref", "column": "has_ad"},
                            "right": {"node_type": "literal", "value_type": "bool", "value": False},
                        },
                        "output": {"flag": "NO_AD"},
                    }
                ],
            },
        ]
    )
    compiled = compile_ruleset_from_payload(payload, ruleset_name="preview", version=1)
    explained = explain_row(compiled, row=_row(has_edr=True, has_ad=False))

    assert explained["final_output"]["primary_status"] == engine_settings.classification_default_primary_status
    assert explained["final_output"]["flags"] == ["NO_AD"]
    assert explained["evaluation_order"] == ["primary:missing_edr", "flags:no_ad"]
    assert explained["decision_reason"].startswith("Nenhuma primary bateu")

    primary_trace = next(item for item in explained["rules"] if item["rule_key"] == "primary:missing_edr")
    assert primary_trace["matched"] is False
    assert primary_trace["failed_conditions"]


def test_dry_run_ruleset_shadow_emits_divergence_warning(monkeypatch) -> None:
    from compliance_gate.Engine.rulesets import runtime as runtime_module

    payload = RuleSetPayloadV2(
        blocks=[
            {
                "kind": "primary",
                "entries": [
                    {
                        "rule_key": "missing_edr",
                        "priority": 1,
                        "condition": {
                            "node_type": "binary_op",
                            "operator": "==",
                            "left": {"node_type": "column_ref", "column": "has_edr"},
                            "right": {"node_type": "literal", "value_type": "bool", "value": False},
                        },
                        "output": {
                            "primary_status": "ROGUE",
                            "primary_status_label": "Rogue",
                        },
                    }
                ],
            }
        ]
    )
    compiled = compile_ruleset_from_payload(payload, ruleset_name="preview", version=1)

    monkeypatch.setattr(
        runtime_module,
        "evaluate_machine",
        lambda *_args, **_kwargs: MachineStatusResult(
            primary_status="COMPLIANT",
            primary_status_label="Compliant",
            flags=[],
        ),
    )

    result = dry_run_ruleset(
        compiled,
        rows=[_row(has_edr=False)],
        mode=ClassificationRuntimeMode.SHADOW,
        explain_sample_limit=1,
    )
    assert result["mode"] == "shadow"
    assert result["divergences"] == 1
    assert result["warnings"][0]["code"] == "ShadowDivergenceWarning"
    assert result["sample_explain"]["explained_rows"] == 1
