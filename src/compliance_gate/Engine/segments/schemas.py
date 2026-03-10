from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from compliance_gate.Engine.expressions import (
    ExpressionDataType,
    ExpressionNode,
    ExpressionValidationOptions,
    validate_expression,
)


class SegmentPayloadV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    filter_expression: ExpressionNode

    def validate_types(
        self,
        *,
        column_types: dict[str, str | ExpressionDataType],
        options: ExpressionValidationOptions | None = None,
    ) -> None:
        validate_expression(
            self.filter_expression,
            column_types=column_types,
            expected_type=ExpressionDataType.BOOL,
            options=options,
        )


class SegmentDefinition(BaseModel):
    id: str
    tenant_id: str
    name: str
    description: str | None = None
    created_by: str | None = None
    created_at: datetime
    active_version: int = Field(default=1, ge=1)


class SegmentVersion(BaseModel):
    id: str
    segment_id: str
    version: int = Field(ge=1)
    payload: SegmentPayloadV1
    created_at: datetime
    created_by: str | None = None

