from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from compliance_gate.Engine.expressions import (
    ExpressionDataType,
    ExpressionNode,
    ExpressionValidationOptions,
    validate_expression,
)


class RuleBlockKind(StrEnum):
    SPECIAL = "special"
    PRIMARY = "primary"
    FLAGS = "flags"


class RuleBlockExecutionMode(StrEnum):
    BYPASS = "bypass"
    FIRST_MATCH_WINS = "first_match_wins"
    ADDITIVE = "additive"


class RuleSetVersionStatus(StrEnum):
    DRAFT = "draft"
    VALIDATED = "validated"
    PUBLISHED = "published"
    ARCHIVED = "archived"


class ClassificationRuntimeMode(StrEnum):
    LEGACY = "legacy"
    SHADOW = "shadow"
    DECLARATIVE = "declarative"


class ClassificationMigrationPhase(StrEnum):
    A = "A"
    B = "B"
    C = "C"
    D = "D"


_BLOCK_MODE_BY_KIND: dict[RuleBlockKind, RuleBlockExecutionMode] = {
    RuleBlockKind.SPECIAL: RuleBlockExecutionMode.BYPASS,
    RuleBlockKind.PRIMARY: RuleBlockExecutionMode.FIRST_MATCH_WINS,
    RuleBlockKind.FLAGS: RuleBlockExecutionMode.ADDITIVE,
}


class RuleEntryPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rule_key: str | None = Field(default=None, max_length=128)
    description: str | None = Field(default=None, max_length=512)
    priority: int = Field(default=0, ge=0, le=1_000_000)
    condition: ExpressionNode
    output: dict[str, Any] = Field(default_factory=dict, min_length=1)


class RuleBlockPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: RuleBlockKind
    entries: list[RuleEntryPayload] = Field(default_factory=list, max_length=10_000)

    @property
    def execution_mode(self) -> RuleBlockExecutionMode:
        return _BLOCK_MODE_BY_KIND[self.kind]

    @model_validator(mode="after")
    def _validate_priority_uniqueness(self) -> RuleBlockPayload:
        if self.kind not in {RuleBlockKind.SPECIAL, RuleBlockKind.PRIMARY}:
            return self

        priorities = [entry.priority for entry in self.entries]
        if len(priorities) != len(set(priorities)):
            raise ValueError(
                "blocos special/primary exigem prioridade única por regra para first-match-wins"
            )
        return self


class RuleSetPayloadV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[2] = 2
    blocks: list[RuleBlockPayload] = Field(default_factory=list, max_length=16)

    @model_validator(mode="after")
    def _validate_block_uniqueness(self) -> RuleSetPayloadV2:
        kinds = [block.kind for block in self.blocks]
        if len(kinds) != len(set(kinds)):
            raise ValueError(
                "cada block kind (special/primary/flags) pode aparecer no máximo uma vez"
            )
        return self

    def block(self, kind: RuleBlockKind) -> RuleBlockPayload | None:
        for block in self.blocks:
            if block.kind == kind:
                return block
        return None

    def blocks_by_kind(self) -> dict[RuleBlockKind, RuleBlockPayload]:
        return {block.kind: block for block in self.blocks}

    def validate_types(
        self,
        *,
        column_types: Mapping[str, str | ExpressionDataType],
        options: ExpressionValidationOptions | None = None,
    ) -> None:
        for block in self.blocks:
            for entry in block.entries:
                validate_expression(
                    entry.condition,
                    column_types=column_types,
                    expected_type=ExpressionDataType.BOOL,
                    options=options,
                )


class RuleSetValidationIssue(BaseModel):
    code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)
    hint: str | None = None
    node_path: str | None = None


class RuleSetValidationResult(BaseModel):
    is_valid: bool
    issues: list[RuleSetValidationIssue] = Field(default_factory=list)
