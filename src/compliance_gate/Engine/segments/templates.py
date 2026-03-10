from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from compliance_gate.Engine.expressions import ExpressionNode, parse_expression_node


class SegmentTemplate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key: str
    name: str
    description: str
    expression: ExpressionNode


_RAW_TEMPLATES: tuple[dict[str, Any], ...] = (
    {
        "key": "status_is_rogue",
        "name": "Status Rogue",
        "description": "Máquinas classificadas como ROGUE.",
        "expression": {
            "node_type": "binary_op",
            "operator": "==",
            "left": {"node_type": "column_ref", "column": "primary_status"},
            "right": {"node_type": "literal", "value_type": "string", "value": "ROGUE"},
        },
    },
    {
        "key": "status_is_gap",
        "name": "Status Gap",
        "description": "Máquinas classificadas como GAP.",
        "expression": {
            "node_type": "binary_op",
            "operator": "==",
            "left": {"node_type": "column_ref", "column": "primary_status"},
            "right": {"node_type": "literal", "value_type": "string", "value": "GAP"},
        },
    },
    {
        "key": "missing_edr",
        "name": "Sem EDR",
        "description": "Máquinas sem presença de EDR.",
        "expression": {
            "node_type": "binary_op",
            "operator": "==",
            "left": {"node_type": "column_ref", "column": "has_edr"},
            "right": {"node_type": "literal", "value_type": "bool", "value": False},
        },
    },
)

SEGMENT_TEMPLATES: tuple[SegmentTemplate, ...] = tuple(
    SegmentTemplate(
        key=raw["key"],
        name=raw["name"],
        description=raw["description"],
        expression=parse_expression_node(raw["expression"]),
    )
    for raw in _RAW_TEMPLATES
)


def list_segment_templates() -> list[SegmentTemplate]:
    return list(SEGMENT_TEMPLATES)


def get_segment_template(template_key: str) -> SegmentTemplate | None:
    for template in SEGMENT_TEMPLATES:
        if template.key == template_key:
            return template
    return None
