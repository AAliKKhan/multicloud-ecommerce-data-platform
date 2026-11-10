"""Explicit warehouse table schemas, independent of SQL dialect.

Each client renders these into its own DDL (DuckDB locally, BigQuery in the
cloud). Raw events land in one typed event ledger; business entities are
derived from it by dbt (see ADR 0007).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum


class ColumnType(StrEnum):
    STRING = "STRING"
    INT = "INT"
    BOOL = "BOOL"
    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"


@dataclass(frozen=True)
class Column:
    name: str
    type: ColumnType
    nullable: bool = True


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[Column, ...]
    partition_by: str | None = None  # BigQuery: DATE column to partition on
    cluster_by: tuple[str, ...] = ()
    expires_hours: int | None = None  # BigQuery: auto-expire scratch tables

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def with_name(self, name: str) -> TableSpec:
        return TableSpec(name, self.columns, self.partition_by, self.cluster_by, self.expires_hours)


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")


def safe_identifier(name: str) -> str:
    """Names are interpolated into SQL, so only plain identifiers are allowed."""
    if not _IDENTIFIER.fullmatch(name):
        raise ValueError(f"unsafe SQL identifier: {name!r}")
    return name


def sanitize_for_identifier(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", value)


RAW_EVENTS = TableSpec(
    name="raw_events",
    columns=(
        Column("event_id", ColumnType.STRING, nullable=False),
        Column("event_type", ColumnType.STRING, nullable=False),
        Column("schema_version", ColumnType.INT, nullable=False),
        Column("event_time", ColumnType.TIMESTAMP, nullable=False),
        Column("event_date", ColumnType.DATE, nullable=False),
        Column("producer", ColumnType.STRING),
        Column("correlation_id", ColumnType.STRING),
        Column("payload", ColumnType.STRING, nullable=False),  # JSON text
        Column("ingested_at", ColumnType.TIMESTAMP, nullable=False),
        Column("is_late", ColumnType.BOOL),
        Column("source_topic", ColumnType.STRING),
        Column("kafka_partition", ColumnType.INT),
        Column("kafka_offset", ColumnType.INT),
        Column("run_id", ColumnType.STRING),
        Column("batch_id", ColumnType.STRING, nullable=False),
        Column("load_id", ColumnType.STRING, nullable=False),
        Column("loaded_at", ColumnType.TIMESTAMP, nullable=False),
    ),
    partition_by="event_date",
    cluster_by=("event_type", "event_id"),
)

LOAD_AUDIT = TableSpec(
    name="load_audit",
    columns=(
        Column("load_id", ColumnType.STRING, nullable=False),
        Column("batch_id", ColumnType.STRING, nullable=False),
        Column("attempt", ColumnType.INT, nullable=False),  # 1, 2, 3... per batch
        Column("source", ColumnType.STRING, nullable=False),
        Column("status", ColumnType.STRING, nullable=False),  # SUCCEEDED | FAILED
        Column("expected_rows", ColumnType.INT, nullable=False),
        Column("rows_read", ColumnType.INT, nullable=False),
        Column("rows_inserted", ColumnType.INT, nullable=False),
        Column("rows_duplicate", ColumnType.INT, nullable=False),
        Column("started_at", ColumnType.TIMESTAMP, nullable=False),
        Column("finished_at", ColumnType.TIMESTAMP, nullable=False),
        Column("error", ColumnType.STRING),
    ),
)
