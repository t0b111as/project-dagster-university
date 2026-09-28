from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.request import urlopen

import dagster as dg
from dagster_duckdb import DuckDBResource


SEED_BASE_URL = (
    "https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/"
    "refs/heads/main/seeds"
)


def _load_csv(
    duckdb: DuckDBResource, filename: str, table_name: str
) -> dg.MaterializeResult:
    with urlopen(f"{SEED_BASE_URL}/{filename}", timeout=30) as response:
        csv_data = response.read()

    with TemporaryDirectory() as temporary_dir:
        csv_path = Path(temporary_dir) / filename
        csv_path.write_bytes(csv_data)

        with duckdb.get_connection() as connection:
            connection.execute(
                f"CREATE OR REPLACE TABLE {table_name} AS "
                "SELECT * FROM read_csv_auto(?)",
                [str(csv_path)],
            )
            row_count = connection.execute(
                f"SELECT count(*) FROM {table_name}"
            ).fetchone()[0]

    return dg.MaterializeResult(
        metadata={
            "table": table_name,
            "row_count": row_count,
            "source_url": f"{SEED_BASE_URL}/{filename}",
        }
    )


@dg.asset(group_name="raw")
def raw_customers(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """Load the Jaffle Shop customers CSV into DuckDB."""
    return _load_csv(duckdb, "raw_customers.csv", "raw_customers")


@dg.asset(group_name="raw")
def raw_orders(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """Load the Jaffle Shop orders CSV into DuckDB."""
    return _load_csv(duckdb, "raw_orders.csv", "raw_orders")


@dg.asset(group_name="raw")
def raw_payments(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """Load the Jaffle Shop payments CSV into DuckDB."""
    return _load_csv(duckdb, "raw_payments.csv", "raw_payments")
