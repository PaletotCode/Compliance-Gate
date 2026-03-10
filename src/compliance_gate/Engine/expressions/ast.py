from __future__ import annotations

from datetime import date
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

from compliance_gate.Engine.expressions.types import LiteralValueType


class LiteralNode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    node_type: Literal["literal"] = "literal"
    value_type: LiteralValueType
    value: Any

    @model_validator(mode="after")
    def _validate_value_type(self) -> LiteralNode:
        if self.value_type == LiteralValueType.STRING and not isinstance(self.value, str):
            raise ValueError("literal value must be string")
        if self.value_type == LiteralValueType.BOOL and not isinstance(self.value, bool):
            raise ValueError("literal value must be bool")
        if self.value_type == LiteralValueType.INT and (
            not isinstance(self.value, int) or isinstance(self.value, bool)
        ):
            raise ValueError("literal value must be int")
        if self.value_type == LiteralValueType.FLOAT and (
            not isinstance(self.value, (int, float)) or isinstance(self.value, bool)
        ):
            raise ValueError("literal value must be float")
        if self.value_type == LiteralValueType.DATE:
            if isinstance(self.value, date):
                return self
            if not isinstance(self.value, str):
                raise ValueError("literal value must be ISO date string")
            try:
                date.fromisoformat(self.value)
            except ValueError as exc:
                raise ValueError("literal date must be ISO format YYYY-MM-DD") from exc
        if self.value_type == LiteralValueType.NULL and self.value is not None:
            raise ValueError("literal value must be null when value_type is null")
        return self


class ColumnRefNode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    node_type: Literal["column_ref"] = "column_ref"
    column: str = Field(min_length=1, max_length=256)


class UnaryOpNode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    node_type: Literal["unary_op"] = "unary_op"
    operator: Literal["NOT"]
    operand: ExpressionNode


class BinaryOpNode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    node_type: Literal["binary_op"] = "binary_op"
    operator: Literal["==", "!=", ">", ">=", "<", "<=", "IN", "+", "-", "*", "/"]
    left: ExpressionNode
    right: ExpressionNode


class LogicalOpNode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    node_type: Literal["logical_op"] = "logical_op"
    operator: Literal["AND", "OR"]
    clauses: list[ExpressionNode] = Field(min_length=2, max_length=128)


class FunctionCallNode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    node_type: Literal["function_call"] = "function_call"
    function_name: Literal[
        "contains",
        "starts_with",
        "ends_with",
        "regex_match",
        "regex_extract",
        "split_part",
        "substring",
        "upper",
        "lower",
        "trim",
        "date_now",
        "now_ms",
        "date_diff",
        "date_diff_days",
        "is_null",
        "is_not_null",
        "coalesce",
        "to_int",
    ]
    arguments: list[ExpressionNode] = Field(min_length=0, max_length=16)


ExpressionNode = Annotated[
    LiteralNode | ColumnRefNode | UnaryOpNode | BinaryOpNode | LogicalOpNode | FunctionCallNode,
    Field(discriminator="node_type"),
]

ExpressionNodeAdapter = TypeAdapter(ExpressionNode)


def parse_expression_node(payload: dict[str, Any]) -> ExpressionNode:
    return ExpressionNodeAdapter.validate_python(payload)


UnaryOpNode.model_rebuild()
BinaryOpNode.model_rebuild()
LogicalOpNode.model_rebuild()
FunctionCallNode.model_rebuild()
