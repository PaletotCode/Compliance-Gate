from __future__ import annotations

import hashlib
import time
from pathlib import Path

import polars as pl

from compliance_gate.Engine.spines.models import EngineBaseMetrics


class ParquetWriter:
    @staticmethod
    def calculate_checksum(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as file_obj:
            while True:
                chunk = file_obj.read(65536)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def write_dataframe(df: pl.DataFrame, target_path: Path) -> tuple[int, str, EngineBaseMetrics]:
        target_path.parent.mkdir(parents=True, exist_ok=True)

        start = time.perf_counter()
        df.write_parquet(str(target_path), compression="snappy")
        elapsed = (time.perf_counter() - start) * 1000

        row_count = df.height
        checksum = ParquetWriter.calculate_checksum(target_path)

        metrics = EngineBaseMetrics(
            elapsed_ms=elapsed,
            row_count=row_count,
            checksum=checksum,
        )
        return row_count, checksum, metrics

    @staticmethod
    def read_schema(path: Path) -> dict[str, str]:
        df = pl.read_parquet(str(path), n_rows=0)
        return {name: str(dtype) for name, dtype in zip(df.columns, df.dtypes, strict=False)}
