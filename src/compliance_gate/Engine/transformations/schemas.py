from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from compliance_gate.Engine.expressions import (
    ExpressionDataType,
    ExpressionNode,
    ExpressionValidationOptions,
    validate_expression,
)


class TransformationOutputType(StrEnum):
    STRING = "string"
    INT = "int"
    BOOL = "bool"
    DATE = "date"


class TransformationPayloadV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    output_column_name: str = Field(min_length=1, max_length=256)
    expression: ExpressionNode
    output_type: TransformationOutputType

    def validate_types(
        self,
        *,
        column_types: dict[str, str | ExpressionDataType],
        options: ExpressionValidationOptions | None = None,
    ) -> None:
        validate_expression(
            self.expression,
            column_types=column_types,
            expected_type=ExpressionDataType(self.output_type.value),
            options=options,
        )


class TransformationDefinition(BaseModel):
    id: str
    tenant_id: str
    name: str
    description: str | None = None
    created_by: str | None = None
    created_at: datetime
    active_version: int = Field(default=1, ge=1)


class TransformationVersion(BaseModel):
    id: str
    transformation_id: str
    version: int = Field(ge=1)
    payload: TransformationPayloadV1
    created_at: datetime
    created_by: str | None = None

