"""DuckDB warehouse client: the **local stand-in** for BigQuery.

It lets the full load -> dbt flow run on a laptop without a cloud account. It
is not BigQuery and does not prove BigQuery behaviour (partitioning,
clustering, cost, quotas); those are covered by rendering tests only.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from ecomm.warehouse.client import WarehouseError
from ecomm.warehouse.schema import ColumnType, TableSpec, safe_identifier

_TYPES = {
    ColumnType.STRING: "VARCHAR",
    ColumnType.INT: "BIGINT",
    ColumnType.BOOL: "BOOLEAN",
    ColumnType.TIMESTAMP: "TIMESTAMP",  # stored as naive UTC
    ColumnType.DATE: "DATE",
}


def render_ddl(schema: str, spec: TableSpec) -> str:
    columns = ",\n  ".join(
        f"{safe_identifier(c.name)} {_TYPES[c.type]}{'' if c.nullable else ' NOT NULL'}"
        for c in spec.columns
    )
    return f'CREATE TABLE IF NOT EXISTS "{schema}"."{safe_identifier(spec.name)}" (\n  {columns}\n)'


_CHUNK_ROWS = 200


def _normalise(value: Any) -> Any:
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(UTC).replace(tzinfo=None)
    return value


class DuckDBClient:
    dialect = "duckdb"

    def __init__(self, path: str | Path = ":memory:", schema: str = "raw") -> None:
        self.schema = safe_identifier(schema)
        self.path = str(path)
        self._con = duckdb.connect(self.path)
        self._con.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}"')

    def close(self) -> None:
        self._con.close()

    def ref(self, table: str) -> str:
        return f'"{self.schema}"."{safe_identifier(table)}"'

    def ensure_table(self, spec: TableSpec) -> None:
        self.execute(render_ddl(self.schema, spec))

    def drop_table(self, table: str) -> None:
        self.execute(f"DROP TABLE IF EXISTS {self.ref(table)}")

    def load_rows(self, spec: TableSpec, rows: Sequence[Mapping[str, Any]]) -> None:
        if not rows:
            return
        names = spec.column_names
        group = "(" + ", ".join("?" for _ in names) + ")"
        head = f"INSERT INTO {self.ref(spec.name)} ({', '.join(names)}) VALUES "
        try:
            for start in range(0, len(rows), _CHUNK_ROWS):  # multi-row inserts: far faster
                chunk = rows[start : start + _CHUNK_ROWS]
                params = [_normalise(row.get(name)) for row in chunk for name in names]
                self._con.execute(head + ", ".join([group] * len(chunk)), params)
        except duckdb.Error as exc:
            raise WarehouseError(f"load into {spec.name} failed: {exc}") from exc

    def execute(self, sql: str) -> None:
        try:
            self._con.execute(sql)
        except duckdb.Error as exc:
            raise WarehouseError(str(exc)) from exc

    def query(self, sql: str) -> list[dict[str, Any]]:
        try:
            cursor = self._con.execute(sql)
            columns = [d[0] for d in cursor.description or []]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
        except duckdb.Error as exc:
            raise WarehouseError(str(exc)) from exc
