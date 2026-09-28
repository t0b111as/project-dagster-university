from pathlib import Path

from dagster_duckdb import DuckDBResource


def duckdb_resource() -> DuckDBResource:
    project_root = Path(__file__).resolve().parents[2]
    return DuckDBResource(database=str(project_root / "data" / "university.duckdb"))
