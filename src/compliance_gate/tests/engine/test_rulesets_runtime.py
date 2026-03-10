from __future__ import annotations

from types import SimpleNamespace

import pytest

from compliance_gate.domains.machines.classification.models import MachineStatusResult
from compliance_gate.Engine.errors import GuardrailViolation
from compliance_gate.Engine.rulesets import (
    ClassificationMigrationPhase,
    ClassificationRuntimeMode,
    RuleSetPayloadV2,
    classify_records,
    compile_ruleset_record,
)


def _record(
    *,
    hostname: str,
    pa_code: str = "PA-1",
    has_ad: bool = True,
    has_uem: bool = True,
    has_edr: bool = True,
    has_asset: bool = True,
) -> dict:
    return {
        "hostname": hostname,
        "pa_code": pa_code,
        "has_ad": has_ad,
        "has_uem": has_uem,
        "has_edr": has_edr,
        "has_asset": has_asset,
    }


def _compiled_ruleset(payload: RuleSetPayloadV2):
    return compile_ruleset_record(
        record=SimpleNamespace(
            definition=SimpleNamespace(name="official-machines"),
            version=SimpleNamespace(version=1),
            payload=payload,
        )
    )


def test_declarative_runtime_applies_precedence_special_primary_flags() -> None:
    compiled = _compiled_ruleset(
        RuleSetPayloadV2(
            blocks=[
                {
                    "kind": "special",
                    "entries": [
                        {
                            "rule_key": "special_gap",
                            "priority": 1,
                            "condition": {
                                "node_type": "function_call",
                                "function_name": "starts_with",
                                "arguments": [
                                    {"node_type": "column_ref", "column": "hostname"},
                                    {"node_type": "literal", "value_type": "string", "value": "SP-"},
                                ],
                            },
                            "output": {
                                "primary_status": "SPECIAL",
                                "primary_status_label": "Special",
                                "flags": ["BYPASS_FLAG"],
                            },
                        }
                    ],
                },
                {
                    "kind": "primary",
                    "entries": [
                        {
                            "rule_key": "primary_missing_edr",
                            "priority": 10,
                            "condition": {
                                "node_type": "binary_op",
                                "operator": "==",
                                "left": {"node_type": "column_ref", "column": "has_edr"},
                                "right": {"node_type": "literal", "value_type": "bool", "value": False},
                            },
                            "output": {
                                "primary_status": "MISSING_EDR",
                                "primary_status_label": "Missing EDR",
                            },
                        },
                        {
                            "rule_key": "primary_missing_uem",
                            "priority": 20,
                            "condition": {
                                "node_type": "binary_op",
                                "operator": "==",
                                "left": {"node_type": "column_ref", "column": "has_uem"},
                                "right": {"node_type": "literal", "value_type": "bool", "value": False},
                            },
                            "output": {
                                "primary_status": "MISSING_UEM",
                                "primary_status_label": "Missing UEM",
                            },
                        },
                    ],
                },
                {
                    "kind": "flags",
                    "entries": [
                        {
                            "rule_key": "flag_no_ad",
                            "priority": 1,
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
    )

    rows = [
        _record(hostname="SP-001", has_ad=False, has_edr=False, has_uem=False),
        _record(hostname="PC-001", has_ad=False, has_edr=False, has_uem=True),
        _record(hostname="PC-002", has_ad=True, has_edr=True, has_uem=False),
    ]

    batch = classify_records(
        rows,
        mode=ClassificationRuntimeMode.DECLARATIVE,
        compiled_ruleset=compiled,
    )

    assert [item.primary_status for item in batch.outputs] == [
        "SPECIAL",
        "MISSING_EDR",
        "MISSING_UEM",
    ]
    assert batch.outputs[0].flags == ["BYPASS_FLAG"]
    assert batch.outputs[1].flags == ["NO_AD"]
    assert batch.outputs[2].flags == []
    assert batch.metrics.rows_scanned == 3
    assert batch.metrics.rows_classified == 3
    assert batch.metrics.rule_hits["special:special_gap"] == 1
    assert batch.metrics.rule_hits["primary:primary_missing_edr"] == 1
    assert batch.metrics.rule_hits["primary:primary_missing_uem"] == 1
    assert batch.metrics.rule_hits["flags:flag_no_ad"] == 1


def test_shadow_mode_keeps_legacy_output_and_records_divergence(monkeypatch) -> None:
    from compliance_gate.Engine.rulesets import runtime as runtime_module

    compiled = _compiled_ruleset(
        RuleSetPayloadV2(
            blocks=[
                {
                    "kind": "primary",
                    "entries": [
                        {
                            "rule_key": "primary_missing_edr",
                            "priority": 10,
                            "condition": {
                                "node_type": "binary_op",
                                "operator": "==",
                                "left": {"node_type": "column_ref", "column": "has_edr"},
                                "right": {"node_type": "literal", "value_type": "bool", "value": False},
                            },
                            "output": {"primary_status": "ROGUE", "primary_status_label": "Rogue"},
                        }
                    ],
                }
            ]
        )
    )

    monkeypatch.setattr(
        runtime_module,
        "evaluate_machine",
        lambda *_args, **_kwargs: MachineStatusResult(
            primary_status="COMPLIANT",
            primary_status_label="Compliant",
            flags=["LEGACY_FLAG"],
        ),
    )

    batch = classify_records(
        [_record(hostname="PC-001", has_edr=False)],
        mode=ClassificationRuntimeMode.SHADOW,
        compiled_ruleset=compiled,
    )

    assert batch.outputs[0].primary_status == "COMPLIANT"
    assert batch.outputs[0].primary_status_label == "Compliant"
    assert batch.outputs[0].flags == ["LEGACY_FLAG"]
    assert batch.metrics.rule_hits["primary:primary_missing_edr"] == 1
    assert batch.metrics.divergences == 1
    assert len(batch.divergences) == 1
    assert batch.divergences[0].diff["primary_status"] == {
        "legacy": "COMPLIANT",
        "declarative": "ROGUE",
    }


def test_legacy_mode_uses_legacy_classifier(monkeypatch) -> None:
    from compliance_gate.Engine.rulesets import runtime as runtime_module

    monkeypatch.setattr(
        runtime_module,
        "evaluate_machine",
        lambda machine, **_kwargs: MachineStatusResult(
            primary_status=f"LEGACY_{machine.hostname}",
            primary_status_label="Legacy Label",
            flags=["LEGACY_FLAG"],
        ),
    )

    batch = classify_records(
        [_record(hostname="HOST-A"), _record(hostname="HOST-B")],
        mode=ClassificationRuntimeMode.LEGACY,
        compiled_ruleset=None,
    )

    assert [item.primary_status for item in batch.outputs] == [
        "LEGACY_HOST-A",
        "LEGACY_HOST-B",
    ]
    assert all(item.flags == ["LEGACY_FLAG"] for item in batch.outputs)
    assert batch.metrics.mode == ClassificationRuntimeMode.LEGACY
    assert batch.metrics.rows_scanned == 2
    assert batch.metrics.rows_classified == 2
    assert batch.metrics.divergences == 0


def test_shadow_requires_compiled_ruleset() -> None:
    with pytest.raises(GuardrailViolation):
        classify_records(
            [_record(hostname="PC-001")],
            mode=ClassificationRuntimeMode.SHADOW,
            compiled_ruleset=None,
        )


def test_declarative_cutover_phases_apply_progressive_output(monkeypatch) -> None:
    from compliance_gate.Engine.rulesets import runtime as runtime_module

    compiled = _compiled_ruleset(
        RuleSetPayloadV2(
            blocks=[
                {
                    "kind": "special",
                    "entries": [
                        {
                            "rule_key": "special_gap",
                            "priority": 1,
                            "condition": {
                                "node_type": "function_call",
                                "function_name": "starts_with",
                                "arguments": [
                                    {"node_type": "column_ref", "column": "hostname"},
                                    {"node_type": "literal", "value_type": "string", "value": "SP-"},
                                ],
                            },
                            "output": {
                                "primary_status": "SPECIAL",
                                "primary_status_label": "Special",
                            },
                        }
                    ],
                },
                {
                    "kind": "primary",
                    "entries": [
                        {
                            "rule_key": "primary_rogue",
                            "priority": 1,
                            "condition": {
                                "node_type": "binary_op",
                                "operator": "==",
                                "left": {"node_type": "column_ref", "column": "has_edr"},
                                "right": {"node_type": "literal", "value_type": "bool", "value": False},
                            },
                            "output": {"primary_status": "ROGUE", "primary_status_label": "Rogue"},
                        }
                    ],
                },
                {
                    "kind": "flags",
                    "entries": [
                        {
                            "rule_key": "flag_no_ad",
                            "priority": 1,
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
    )

    def fake_legacy(machine, **_kwargs):
        if machine.hostname.startswith("SP-"):
            return MachineStatusResult(primary_status="GAP", primary_status_label="Gap", flags=[])
        return MachineStatusResult(
            primary_status="COMPLIANT",
            primary_status_label="Compliant",
            flags=["LEGACY_FLAG"],
        )

    monkeypatch.setattr(runtime_module, "evaluate_machine", fake_legacy)

    rows = [
        _record(hostname="SP-001", has_ad=False, has_edr=False, has_uem=False),
        _record(hostname="PC-001", has_ad=False, has_edr=False, has_uem=True),
    ]

    phase_a = classify_records(
        rows,
        mode=ClassificationRuntimeMode.DECLARATIVE,
        compiled_ruleset=compiled,
        cutover_phase=ClassificationMigrationPhase.A,
    )
    assert phase_a.outputs[0].primary_status == "GAP"
    assert phase_a.outputs[0].flags == []
    assert phase_a.outputs[1].primary_status == "COMPLIANT"
    assert phase_a.outputs[1].flags == ["NO_AD"]

    phase_b = classify_records(
        rows,
        mode=ClassificationRuntimeMode.DECLARATIVE,
        compiled_ruleset=compiled,
        cutover_phase=ClassificationMigrationPhase.B,
    )
    assert phase_b.outputs[0].primary_status == "GAP"
    assert phase_b.outputs[1].primary_status == "ROGUE"
    assert phase_b.outputs[1].flags == ["NO_AD"]

    phase_c = classify_records(
        rows,
        mode=ClassificationRuntimeMode.DECLARATIVE,
        compiled_ruleset=compiled,
        cutover_phase=ClassificationMigrationPhase.C,
    )
    assert phase_c.outputs[0].primary_status == "SPECIAL"
    assert phase_c.outputs[1].primary_status == "ROGUE"
