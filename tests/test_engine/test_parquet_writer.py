"""
test_parquet_writer.py - Verifies DuckDB mappings to Parquet file generation.
"""
import os
import duckdb
from compliance_gate.Engine.materialization.parquet_writer import ParquetWriter

def test_parquet_writer_basic(tmp_path):
    target = str(tmp_path / "test_out.parquet")
    
    # We run a mock DuckDB query
    query = "SELECT 1 as id, 'mock' as name UNION ALL SELECT 2, 'data'"
    
    row_count, checksum, metrics = ParquetWriter.write_from_query(query, target)
    
    assert row_count == 2
    assert checksum != ""
    assert os.path.exists(target)
    assert metrics.elapsed_ms > 0
    
    # Validate the contents via DuckDB reading back
    conn = duckdb.connect(':memory:')
    res = conn.execute(f"SELECT COUNT(*) FROM '{target}'").fetchone()[0]
    assert res == 2
    
def test_deterministic_checksum(tmp_path):
    target1 = str(tmp_path / "test_out1.parquet")
    target2 = str(tmp_path / "test_out2.parquet")
    
    query = "SELECT 'constant' as val"
    
    _, checksum1, _ = ParquetWriter.write_from_query(query, target1)
    _, checksum2, _ = ParquetWriter.write_from_query(query, target2)
    
    assert checksum1 == checksum2
