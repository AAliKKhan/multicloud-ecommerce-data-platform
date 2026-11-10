"""Warehouse client interface (the loader depends only on this)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Protocol

from ecomm.warehouse.schema import TableSpec


class WarehouseError(Exception):
    """A warehouse operation failed."""


class WarehouseClient(Protocol):
    dialect: str  # "duckdb" | "bigquery"

    def ref(self, table: str) -> str:
        """Fully qualified, quoted reference for a table in the raw dataset."""
        ...

    def ensure_table(self, spec: TableSpec) -> None: ...

    def drop_table(self, table: str) -> None: ...

    def load_rows(self, spec: TableSpec, rows: Sequence[Mapping[str, Any]]) -> None:
        """Append rows (a bulk load, not row-by-row inserts)."""
        ...

    def execute(self, sql: str) -> None: ...

    def query(self, sql: str) -> list[dict[str, Any]]: ...


def utcnow() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)
