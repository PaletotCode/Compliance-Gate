from __future__ import annotations

from compliance_gate.Engine.rulesets import (
    build_legacy_baseline_ruleset_payload,
    build_legacy_rule_templates,
)
from compliance_gate.Engine.rulesets.template_library import list_legacy_rule_inventory


def test_template_library_covers_all_legacy_rules_and_precedence() -> None:
    templates = build_legacy_rule_templates()
    assert len(templates) == 14

    keys = {template.rule_key for template in templates}
    assert keys == {
        "special_gap",
        "special_available",
        "primary_inconsistency",
        "primary_phantom",
        "primary_rogue",
        "primary_missing_uem",
        "primary_missing_edr",
        "primary_missing_asset",
        "primary_swap",
        "primary_clone",
        "primary_offline",
        "primary_compliant",
        "flag_legacy_os",
        "flag_pa_mismatch",
    }

    primary = [template for template in templates if template.block_kind.value == "primary"]
    assert [item.precedence for item in primary] == list(range(1, 11))


def test_baseline_payload_preserves_orchestrator_order_and_functions() -> None:
    payload = build_legacy_baseline_ruleset_payload()
    kinds = [block.kind.value for block in payload.blocks]
    assert kinds == ["special", "primary", "flags"]

    primary = payload.blocks[1]
    assert [entry.rule_key for entry in primary.entries] == [
        "primary_inconsistency",
        "primary_phantom",
        "primary_rogue",
        "primary_missing_uem",
        "primary_missing_edr",
        "primary_missing_asset",
        "primary_swap",
        "primary_clone",
        "primary_offline",
        "primary_compliant",
    ]

    functions = _collect_function_names(payload.model_dump())
    assert "now_ms" in functions
    assert "to_int" in functions


def test_inventory_rows_are_stable_for_frontend_consumption() -> None:
    rows = list_legacy_rule_inventory()
    assert len(rows) == 14
    row = rows[0]
    assert {
        "rule_key",
        "block_kind",
        "precedence",
        "legacy_module",
        "status_key",
        "status_label",
        "severity",
        "is_flag",
        "description",
        "condition",
        "output",
    }.issubset(set(row.keys()))


def _collect_function_names(node) -> set[str]:
    names: set[str] = set()
    if isinstance(node, dict):
        if node.get("node_type") == "function_call" and isinstance(node.get("function_name"), str):
            names.add(node["function_name"])
        for value in node.values():
            names.update(_collect_function_names(value))
    elif isinstance(node, list):
        for item in node:
            names.update(_collect_function_names(item))
    return names
