"""Offline syntax check of the BigQuery variants of the dbt models.

No cloud account is involved: dbt compiles with a throwaway generated key
(never used to authenticate) and the rendered SQL is parsed as BigQuery SQL by
sqlglot. This catches dialect mistakes, but it is NOT a substitute for running
the models on real BigQuery.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import sqlglot

from dbt_support import copy_project, run_dbt, throwaway_keyfile_json

pytestmark = pytest.mark.dbt


@pytest.fixture(scope="module")
def compiled_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("bq")
    project = copy_project(root)
    result = run_dbt(
        project,
        "compile",
        "--target",
        "bigquery_offline",
        "--no-populate-cache",
        env={"DBT_BQ_OFFLINE_KEYFILE_JSON": throwaway_keyfile_json(), "BIGQUERY_DATASET": "ecomm"},
    )
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-1000:]
    return project / "target" / "compiled" / "ecomm" / "models"


LAYERS = {"staging", "intermediate", "marts"}


def model_sql(compiled_dir: Path) -> dict[str, str]:
    """Compiled models only (generated schema-test SQL lives in schema.yml subfolders)."""
    return {
        p.stem: p.read_text(encoding="utf-8")
        for p in compiled_dir.rglob("*.sql")
        if p.parent.name in LAYERS
    }


def all_sql(compiled_dir: Path) -> dict[str, str]:
    return {str(p): p.read_text(encoding="utf-8") for p in compiled_dir.rglob("*.sql")}


def test_every_model_compiled(compiled_dir: Path) -> None:
    names = set(model_sql(compiled_dir))
    assert {"stg_orders", "stg_order_items", "fct_orders", "mart_daily_sales"} <= names
    assert len(names) == 19  # 6 staging + 3 intermediate + 10 marts


def test_compiled_sql_parses_as_bigquery(compiled_dir: Path) -> None:
    checked = all_sql(compiled_dir)
    assert len(checked) > 19  # models plus generated data-test queries
    for name, sql in checked.items():
        try:
            sqlglot.parse_one(sql, read="bigquery")
        except sqlglot.errors.ParseError as exc:
            pytest.fail(f"{name} is not valid BigQuery SQL: {exc}")


def test_no_duckdb_only_functions_leak_into_bigquery_sql(compiled_dir: Path) -> None:
    for name, sql in model_sql(compiled_dir).items():
        for duckdb_only in ("json_extract_string", "strptime(", "json_extract(", "json[]"):
            assert duckdb_only not in sql, f"{name} contains DuckDB-only syntax {duckdb_only!r}"


def test_bigquery_json_functions_are_used(compiled_dir: Path) -> None:
    sql = model_sql(compiled_dir)
    assert "json_value(" in sql["stg_orders"]
    assert "json_query_array(" in sql["stg_order_items"]
    assert "timestamp(" in sql["stg_orders"]
    assert "numeric" in sql["stg_orders"]
