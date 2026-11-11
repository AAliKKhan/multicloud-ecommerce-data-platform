"""Publish the curated marts to the BI database that Metabase reads.

Why a copy: DuckDB (the local stand-in for BigQuery) allows either one writer or many read-only
openers of a file, never both. A BI tool holding the warehouse open would block the pipeline's
loads. So the pipeline publishes the finished marts into Postgres, a first-class Metabase source.
In the cloud Metabase would connect to BigQuery directly and this step would not exist.

The replacement is one transaction: Metabase sees the previous marts or the new ones, never a mix
or an empty table. Only the curated marts and one freshness row are published, never raw events.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

SCHEMA = "analytics"

MARTS: dict[str, str] = {
    "mart_daily_sales": "Daily orders, GMV, discounts, refunds, net sales and AOV.",
    "mart_payment_health": "Daily payment outcomes: success rate, failures, refunds, amounts.",
    "mart_product_performance": "Units and sales per product (qualifying orders only).",
    "mart_inventory_risk": "Units on hand per product and location with a stock status.",
}
FRESHNESS_TABLE = "data_freshness"
FRESHNESS_COMMENT = (
    "One row: when the warehouse last loaded successfully and when this copy was made."
)
DEMO_NOTICE = " SYNTHETIC DEMO DATA."

_DECIMAL = re.compile(r"^DECIMAL\((\d+),\s*(\d+)\)$")
_SIMPLE_TYPES = {
    "DATE": "date",
    "TIMESTAMP": "timestamp",
    "BIGINT": "bigint",
    "HUGEINT": "bigint",  # counts and sums here; an overflow fails loudly rather than wrapping
    "INTEGER": "integer",
    "SMALLINT": "smallint",
    "DOUBLE": "double precision",
    "FLOAT": "real",
    "VARCHAR": "text",
    "BOOLEAN": "boolean",
}

_FRESHNESS_QUERY = """
    select
        max(finished_at) filter (where status = 'SUCCEEDED'),
        count(distinct batch_id) filter (where status = 'SUCCEEDED'),
        count(*) filter (where status = 'FAILED')
    from raw.load_audit
"""


class PublishError(RuntimeError):
    """The marts could not be published (the message says what is missing)."""


@dataclass(frozen=True)
class PublishResult:
    rows: dict[str, int]
    published_at: datetime


def postgres_type(duckdb_type: str) -> str:
    """Map a DuckDB column type to Postgres; an unknown type is an error, never a guess."""
    upper = duckdb_type.upper()
    if upper in _SIMPLE_TYPES:
        return _SIMPLE_TYPES[upper]
    match = _DECIMAL.match(upper)
    if match:
        return f"numeric({match.group(1)},{match.group(2)})"
    raise PublishError(f"no Postgres mapping for DuckDB type {duckdb_type!r}")


def _fetch_mart(
    con: duckdb.DuckDBPyConnection, table: str
) -> tuple[list[tuple[str, str]], list[tuple[Any, ...]]]:
    if table not in MARTS:  # the name is interpolated below: only known marts are allowed
        raise PublishError(f"{table} is not a curated mart")
    try:
        columns = [
            (str(row[0]), str(row[1]))
            for row in con.execute(f"describe analytics.{table}").fetchall()  # noqa: S608
        ]
        rows = con.execute(f"select * from analytics.{table}").fetchall()  # noqa: S608
    except duckdb.Error as exc:
        raise PublishError(
            f"mart analytics.{table} is not readable ({type(exc).__name__}). "
            "Run the warehouse load and `ecomm-dbt build` first."
        ) from exc
    return columns, rows


def _replace_table(
    cur: psycopg.Cursor[Any],
    table: str,
    columns: list[tuple[str, str]],
    rows: list[tuple[Any, ...]],
    comment: str,
) -> None:
    target = sql.Identifier(SCHEMA, table)
    definition = sql.SQL(", ").join(
        sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(postgres_type(kind)))
        for name, kind in columns
    )
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(target))
    cur.execute(sql.SQL("CREATE TABLE {} ({})").format(target, definition))
    with cur.copy(sql.SQL("COPY {} FROM STDIN").format(target)) as copy:
        for row in rows:
            copy.write_row(row)
    cur.execute(
        sql.SQL("COMMENT ON TABLE {} IS {}").format(target, sql.Literal(comment + DEMO_NOTICE))
    )


def publish_marts(
    warehouse: Path,
    connection: dict[str, Any],
    *,
    now: datetime | None = None,
) -> PublishResult:
    """Copy the marts (and a freshness row) from the DuckDB warehouse into Postgres, atomically."""
    if not warehouse.is_file():
        raise PublishError(f"warehouse not found: {warehouse}. Run `ecomm-demo` first.")
    published_at = (now or datetime.now(UTC)).astimezone(UTC).replace(tzinfo=None)

    # Read everything first: if a mart is missing we fail before touching Postgres at all.
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        marts = {table: _fetch_mart(con, table) for table in MARTS}
        last_ok, loaded, failed = con.execute(_FRESHNESS_QUERY).fetchone() or (None, 0, 0)
    except duckdb.Error as exc:
        raise PublishError(f"cannot read the warehouse ({type(exc).__name__}: {exc})") from exc
    finally:
        con.close()

    freshness_columns = [
        ("last_successful_load_at", "TIMESTAMP"),
        ("batches_loaded", "BIGINT"),
        ("failed_load_attempts", "BIGINT"),
        ("published_at", "TIMESTAMP"),
    ]
    freshness_rows = [(last_ok, int(loaded), int(failed), published_at)]

    counts: dict[str, int] = {}
    try:
        conninfo = make_conninfo(**connection)
        with psycopg.connect(conninfo) as conn, conn.cursor() as cur:  # one transaction
            for table, (columns, rows) in marts.items():
                _replace_table(cur, table, columns, rows, MARTS[table])
                counts[table] = len(rows)
            _replace_table(
                cur, FRESHNESS_TABLE, freshness_columns, freshness_rows, FRESHNESS_COMMENT
            )
            counts[FRESHNESS_TABLE] = 1
            for table, expected in counts.items():  # verify before the commit
                cur.execute(
                    sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(SCHEMA, table))
                )
                actual = (cur.fetchone() or (None,))[0]
                if actual != expected:
                    raise PublishError(f"{table}: wrote {expected} rows but found {actual}")
    except psycopg.Error as exc:
        raise PublishError(f"cannot publish to Postgres ({type(exc).__name__}: {exc})") from exc
    return PublishResult(rows=counts, published_at=published_at)
