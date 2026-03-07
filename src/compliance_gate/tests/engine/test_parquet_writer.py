from __future__ import annotations

import polars as pl

from compliance_gate.Engine.materialization.parquet_writer import ParquetWriter


def test_parquet_writer_generates_file_and_checksum(tmp_path) -> None:
    target = tmp_path / "machines_final.parquet"
    df = pl.DataFrame(
        {
            "machine_id": ["a", "b"],
            "primary_status": ["COMPLIANT", "ROGUE"],
        }
    )

    row_count, checksum, metrics = ParquetWriter.write_dataframe(df, target)

    assert target.exists()
    assert row_count == 2
    assert checksum
    assert metrics.row_count == 2
    assert metrics.checksum == checksum


def test_parquet_writer_checksum_is_deterministic(tmp_path) -> None:
    df = pl.DataFrame({"x": [1, 2, 3]})
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"

    _, checksum_a, _ = ParquetWriter.write_dataframe(df, a)
    _, checksum_b, _ = ParquetWriter.write_dataframe(df, b)

    assert checksum_a == checksum_b
