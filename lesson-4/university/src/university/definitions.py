import os
from pathlib import Path

from dagster import definitions, load_from_defs_folder

from university.resources import duckdb_resource


@definitions
def defs():
    project_root = Path(__file__).resolve().parents[2]
    os.environ["UNIVERSITY_DUCKDB_PATH"] = str(project_root / "data" / "university.duckdb")
    return load_from_defs_folder(path_within_project=Path(__file__).parent).with_resources(
        {"duckdb": duckdb_resource()}
    )
