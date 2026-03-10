from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import polars as pl

from compliance_gate.Engine.expressions import parse_expression_node
from compliance_gate.Engine.runtime import preview_segment, run_view
from compliance_gate.Engine.views import ViewPayloadV1


class _ArtifactQuery:
    def __init__(self, path: Path):
        self._path = path

    def filter(self, *_args, **_kwargs):
        return self

    def first(self):
        return SimpleNamespace(path=str(self._path))


class _FakeDB:
    def __init__(self, path: Path):
        self._path = path
        self.added: list[object] = []

    def query(self, *_args, **_kwargs):
        return _ArtifactQuery(self._path)

    def add(self, obj):
        self.added.append(obj)

    def flush(self) -> None:
        return None

    def commit(self) -> None:
        return None


def test_preview_segment_inline_expression(tmp_path: Path) -> None:
    target = tmp_path / "machines_final.parquet"
    pl.DataFrame(
        {
            "hostname": ["HOST-01", "HOST-02", "HOST-03"],
            "primary_status": ["ROGUE", "COMPLIANT", "ROGUE"],
            "has_edr": [False, True, True],
        }
    ).write_parquet(target)

    db = _FakeDB(target)
    expression = parse_expression_node(
        {
            "node_type": "binary_op",
            "operator": "==",
            "left": {"node_type": "column_ref", "column": "primary_status"},
            "right": {"node_type": "literal", "value_type": "string", "value": "ROGUE"},
        }
    )

    result = preview_segment(
        db,
        tenant_id="default",
        dataset_version_id="dataset-1",
        expression=expression,
        limit=10,
    )

    assert result.total_rows == 3
    assert result.matched_rows == 2
    assert result.match_rate == 0.6667
    assert len(result.sample_rows) == 2
    assert any(getattr(run, "run_type", "") == "segment_preview" for run in db.added)


def test_run_view_with_base_columns(tmp_path: Path, monkeypatch) -> None:
    from compliance_gate.Engine.runtime import declarative_runtime as runtime

    target = tmp_path / "machines_final.parquet"
    pl.DataFrame(
        {
            "hostname": ["HOST-02", "HOST-01", "HOST-03"],
            "primary_status": ["ROGUE", "COMPLIANT", "ROGUE"],
            "has_edr": [False, True, True],
        }
    ).write_parquet(target)

    payload = ViewPayloadV1(
        dataset_scope={"dataset_version_id": "dataset-1"},
        columns=[
            {"kind": "base", "column_name": "hostname"},
            {"kind": "base", "column_name": "primary_status"},
        ],
        row_limit=100,
        sort={"column_name": "hostname", "direction": "asc"},
    )

    monkeypatch.setattr(runtime, "get_view", lambda *_args, **_kwargs: SimpleNamespace(payload=payload))

    db = _FakeDB(target)
    result = run_view(
        db,
        tenant_id="default",
        dataset_version_id="dataset-1",
        view_id="view-1",
        page=1,
        size=2,
    )

    assert result.total_rows == 3
    assert result.page == 1
    assert result.size == 2
    assert len(result.items) == 2
    assert result.items[0]["hostname"] == "HOST-01"
    assert any(getattr(run, "run_type", "") == "view_run" for run in db.added)

