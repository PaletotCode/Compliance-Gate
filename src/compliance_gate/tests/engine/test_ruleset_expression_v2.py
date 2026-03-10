from __future__ import annotations

import pytest

from compliance_gate.Engine.errors import (
    InvalidExpressionSyntax,
    RegexCompileError,
    UnsupportedOperatorForType,
)
from compliance_gate.Engine.expressions import ExpressionDataType, parse_expression_node
from compliance_gate.Engine.expressions.validator import validate_expression


def test_expression_v2_supports_null_checks() -> None:
    expression = parse_expression_node(
        {
            "node_type": "function_call",
            "function_name": "is_null",
            "arguments": [{"node_type": "column_ref", "column": "serial"}],
        }
    )

    result = validate_expression(expression, column_types={"serial": "string"})
    assert result == ExpressionDataType.BOOL


def test_expression_v2_supports_contains_for_string() -> None:
    expression = parse_expression_node(
        {
            "node_type": "function_call",
            "function_name": "contains",
            "arguments": [
                {"node_type": "column_ref", "column": "hostname"},
                {"node_type": "literal", "value_type": "string", "value": "srv"},
            ],
        }
    )

    result = validate_expression(expression, column_types={"hostname": "string"})
    assert result == ExpressionDataType.BOOL


def test_expression_v2_supports_contains_for_list() -> None:
    expression = parse_expression_node(
        {
            "node_type": "function_call",
            "function_name": "contains",
            "arguments": [
                {"node_type": "column_ref", "column": "flags"},
                {"node_type": "literal", "value_type": "string", "value": "PERIGO"},
            ],
        }
    )

    result = validate_expression(expression, column_types={"flags": "list[string]"})
    assert result == ExpressionDataType.BOOL


def test_expression_v2_regex_match_rejects_invalid_pattern() -> None:
    expression = parse_expression_node(
        {
            "node_type": "function_call",
            "function_name": "regex_match",
            "arguments": [
                {"node_type": "column_ref", "column": "hostname"},
                {"node_type": "literal", "value_type": "string", "value": "["},
            ],
        }
    )

    with pytest.raises(RegexCompileError):
        validate_expression(expression, column_types={"hostname": "string"})


def test_expression_v2_supports_date_diff_and_date_now() -> None:
    expression = parse_expression_node(
        {
            "node_type": "function_call",
            "function_name": "date_diff",
            "arguments": [
                {"node_type": "column_ref", "column": "last_seen"},
                {
                    "node_type": "function_call",
                    "function_name": "date_now",
                    "arguments": [],
                },
                {"node_type": "literal", "value_type": "string", "value": "days"},
            ],
        }
    )

    result = validate_expression(expression, column_types={"last_seen": "date"})
    assert result == ExpressionDataType.INT


def test_expression_v2_date_diff_rejects_invalid_unit() -> None:
    expression = parse_expression_node(
        {
            "node_type": "function_call",
            "function_name": "date_diff",
            "arguments": [
                {"node_type": "column_ref", "column": "last_seen"},
                {"node_type": "literal", "value_type": "string", "value": "now"},
                {"node_type": "literal", "value_type": "string", "value": "fortnights"},
            ],
        }
    )

    with pytest.raises(InvalidExpressionSyntax):
        validate_expression(expression, column_types={"last_seen": "date"})


def test_expression_v2_supports_math_basic_and_numeric_inference() -> None:
    expression = parse_expression_node(
        {
            "node_type": "binary_op",
            "operator": "/",
            "left": {"node_type": "column_ref", "column": "count_a"},
            "right": {"node_type": "column_ref", "column": "count_b"},
        }
    )

    result = validate_expression(
        expression,
        column_types={"count_a": "int", "count_b": "int"},
    )
    assert result == ExpressionDataType.FLOAT


def test_expression_v2_math_rejects_non_numeric_types() -> None:
    expression = parse_expression_node(
        {
            "node_type": "binary_op",
            "operator": "+",
            "left": {"node_type": "column_ref", "column": "hostname"},
            "right": {"node_type": "literal", "value_type": "string", "value": "-x"},
        }
    )

    with pytest.raises(UnsupportedOperatorForType):
        validate_expression(expression, column_types={"hostname": "string"})


def test_expression_v2_supports_coalesce_multiple_arguments() -> None:
    expression = parse_expression_node(
        {
            "node_type": "function_call",
            "function_name": "coalesce",
            "arguments": [
                {"node_type": "literal", "value_type": "null", "value": None},
                {"node_type": "column_ref", "column": "priority"},
                {"node_type": "literal", "value_type": "int", "value": 7},
            ],
        }
    )

    result = validate_expression(expression, column_types={"priority": "int"})
    assert result == ExpressionDataType.INT


def test_expression_v2_supports_now_ms_and_to_int() -> None:
    expression = parse_expression_node(
        {
            "node_type": "binary_op",
            "operator": ">",
            "left": {
                "node_type": "binary_op",
                "operator": "-",
                "left": {
                    "node_type": "function_call",
                    "function_name": "now_ms",
                    "arguments": [],
                },
                "right": {"node_type": "column_ref", "column": "last_seen_date_ms"},
            },
            "right": {"node_type": "literal", "value_type": "int", "value": 1000},
        }
    )
    result = validate_expression(expression, column_types={"last_seen_date_ms": "int"})
    assert result == ExpressionDataType.BOOL

    cast_expr = parse_expression_node(
        {
            "node_type": "function_call",
            "function_name": "to_int",
            "arguments": [
                {"node_type": "literal", "value_type": "string", "value": "42"},
            ],
        }
    )
    cast_result = validate_expression(cast_expr, column_types={})
    assert cast_result == ExpressionDataType.INT
