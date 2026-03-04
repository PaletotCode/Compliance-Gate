import polars as pl
import duckdb

"""
Stub for dataset storage engine logic using polars and duckdb.
Designed for on-premise execution.
"""

def get_duckdb_conn():
    # Use memory database by default for now
    return duckdb.connect(database=":memory:")
