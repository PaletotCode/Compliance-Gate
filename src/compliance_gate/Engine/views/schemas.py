from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from compliance_gate.Engine.config.engine_settings import engine_settings
from compliance_gate.Engine.errors import GuardrailViolation
from compliance_gate.Engine.expressions import (
    ExpressionDataType,
    ExpressionNode,
    ExpressionValidationOptions,
    validate_expression,
)

DEFAULT_VIEW_ROW_LIMIT = 1_000


class SortDirection(StrEnum):
    ASC = "asc"
    DESC = "desc"


class ViewDatasetScopeV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["dataset_version"] = "dataset_version"
    dataset_version_id: str = Field(min_length=1, max_length=36)


class ViewBaseColumn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["base"] = "base"
    column_name: str = Field(min_length=1, max_length=256)


class ViewDerivedColumn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["derived"] = "derived"
    transformation_id: str = Field(min_length=1, max_length=36)
    alias: str | None = Field(default=None, max_length=256)


ViewColumnSpec = Annotated[ViewBaseColumn | ViewDerivedColumn, Field(discriminator="kind")]


class ViewFilterSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    segment_ids: list[str] = Field(default_factory=list, max_length=128)
    ad_hoc_expression: ExpressionNode | None = None


class ViewSortSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    column_name: str = Field(min_length=1, max_length=256)
    direction: SortDirection = SortDirection.ASC


class ViewPayloadV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    dataset_scope: ViewDatasetScopeV1
    columns: list[ViewColumnSpec] = Field(min_length=1, max_length=512)
    filters: ViewFilterSpec = Field(default_factory=ViewFilterSpec)
    sort: ViewSortSpec | None = None
    row_limit: int = Field(default=DEFAULT_VIEW_ROW_LIMIT, ge=1)

    def validate_guardrails(self, *, max_row_limit: int = engine_settings.max_report_rows) -> None:
        if self.row_limit > max_row_limit:
            raise GuardrailViolation(
                "row_limit acima do limite permitido.",
                details={
                    "row_limit": self.row_limit,
                    "max_row_limit": max_row_limit,
                },
                hint="Reduza o row_limit para um valor suportado pela engine.",
            )

    def validate_types(
        self,
        *,
        column_types: dict[str, str | ExpressionDataType],
        options: ExpressionValidationOptions | None = None,
    ) -> None:
        if self.filters.ad_hoc_expression is None:
            return
        validate_expression(
            self.filters.ad_hoc_expression,
            column_types=column_types,
            expected_type=ExpressionDataType.BOOL,
            options=options,
        )


class ViewDefinition(BaseModel):
    id: str
    tenant_id: str
    name: str
    description: str | None = None
    created_by: str | None = None
    created_at: datetime
    active_version: int = Field(default=1, ge=1)


class ViewVersion(BaseModel):
    id: str
    view_id: str
    version: int = Field(ge=1)
    payload: ViewPayloadV1
    created_at: datetime
    created_by: str | None = None

