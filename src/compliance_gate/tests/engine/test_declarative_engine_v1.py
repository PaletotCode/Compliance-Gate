from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from compliance_gate.Engine.catalog.machines_final import get_machines_final_catalog
from compliance_gate.Engine.config.engine_settings import engine_settings
from compliance_gate.Engine.errors import (
    GuardrailViolation,
    RegexCompileError,
    TypeMismatch,
    UnknownColumn,
)
from compliance_gate.Engine.expressions import parse_expression_node
from compliance_gate.Engine.expressions.types import ExpressionDataType
from compliance_gate.Engine.expressions.validator import validate_expression
from compliance_gate.Engine.segments import SegmentPayloadV1
from compliance_gate.Engine.transformations import TransformationPayloadV1
from compliance_gate.Engine.views import ViewPayloadV1


def test_expression_validator_unknown_column() -> None:
    expression = parse_expression_node({"node_type": "column_ref", "column": "not_exists"})

    with pytest.raises(UnknownColumn):
        validate_expression(expression, column_types={"hostname": ExpressionDataType.STRING})


def test_expression_validator_regex_compile_error() -> None:
    expression = parse_expression_node(
        {
            "node_type": "function_call",
            "function_name": "regex_extract",
            "arguments": [
                {"node_type": "column_ref", "column": "hostname"},
                {"node_type": "literal", "value_type": "string", "value": "["},
                {"node_type": "literal", "value_type": "int", "value": 0},
            ],
        }
    )

    with pytest.raises(RegexCompileError):
        validate_expression(expression, column_types={"hostname": ExpressionDataType.STRING})


def test_transformation_payload_v1_type_validation() -> None:
    payload = TransformationPayloadV1(
        output_column_name="hostname_upper",
        output_type="string",
        expression=parse_expression_node(
            {
                "node_type": "function_call",
                "function_name": "upper",
                "arguments": [{"node_type": "column_ref", "column": "hostname"}],
            }
        ),
    )

    payload.validate_types(column_types={"hostname": "string"})


def test_segment_payload_v1_requires_boolean_expression() -> None:
    payload = SegmentPayloadV1(
        filter_expression=parse_expression_node({"node_type": "column_ref", "column": "hostname"})
    )

    with pytest.raises(TypeMismatch):
        payload.validate_types(column_types={"hostname": "string"})


def test_view_payload_v1_row_limit_guardrail() -> None:
    payload = ViewPayloadV1(
        dataset_scope={"dataset_version_id": "dataset-1"},
        columns=[{"kind": "base", "column_name": "hostname"}],
        row_limit=engine_settings.max_report_rows + 1,
    )

    with pytest.raises(GuardrailViolation):
        payload.validate_guardrails()


def test_machines_final_catalog_snapshot_has_stats(tmp_path: Path) -> None:
    target = tmp_path / "machines_final.parquet"
    pl.DataFrame(
        {
            "hostname": ["A", "B", None, "C"],
            "primary_status": ["COMPLIANT", "ROGUE", "ROGUE", "COMPLIANT"],
            "flags": [["OFFLINE"], [], ["PERIGO"], None],
        }
    ).write_parquet(target)

    class _ArtifactQuery:
        def filter(self, *_args, **_kwargs):
            return self

        def first(self):
            return SimpleNamespace(path=str(target))

    class _FakeDB:
        def query(self, *_args, **_kwargs):
            return _ArtifactQuery()

    snapshot = get_machines_final_catalog(
        _FakeDB(),
        tenant_id="default",
        dataset_version_id="dataset-1",
        sample_size=3,
    )

    assert snapshot.row_count == 4
    assert len(snapshot.columns) == 3
    hostname_col = next(col for col in snapshot.columns if col.name == "hostname")
    assert hostname_col.data_type == "string"
    assert hostname_col.null_rate == 0.25
    assert hostname_col.approx_cardinality >= 3

