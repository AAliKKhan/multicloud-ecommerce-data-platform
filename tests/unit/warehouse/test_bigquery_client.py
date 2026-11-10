"""BigQuery client tests: DDL text and call flow against a fake. NOT a cloud test."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest

from ecomm.warehouse.bigquery_client import BigQueryClient, render_ddl
from ecomm.warehouse.client import WarehouseError
from ecomm.warehouse.duckdb_client import render_ddl as duckdb_ddl
from ecomm.warehouse.schema import LOAD_AUDIT, RAW_EVENTS, safe_identifier


class FakeJob:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = rows or []

    def result(self) -> list[dict[str, Any]]:
        return self.rows


class FakeBigQuery:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.loads: list[tuple[list[dict[str, Any]], str]] = []
        self.fail = False

    def query(self, sql: str) -> FakeJob:
        if self.fail:
            raise RuntimeError("boom with secret details")
        self.statements.append(sql)
        return FakeJob([{"n": 3}])

    def load_table_from_json(
        self, rows: list[dict[str, Any]], table: str, job_config: Any
    ) -> FakeJob:
        self.loads.append((rows, table))
        return FakeJob()


def client(fake: FakeBigQuery) -> BigQueryClient:
    return BigQueryClient("my-proj", "ecomm_raw", client=fake)


def test_event_ledger_ddl_is_partitioned_clustered_and_typed() -> None:
    ddl = render_ddl("my-proj", "ecomm_raw", RAW_EVENTS)
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS `my-proj.ecomm_raw.raw_events`")
    assert "event_id STRING NOT NULL" in ddl
    assert "event_time TIMESTAMP NOT NULL" in ddl
    assert "event_date DATE NOT NULL" in ddl
    assert "kafka_offset INT64" in ddl
    assert "PARTITION BY event_date" in ddl
    assert "CLUSTER BY event_type, event_id" in ddl


def test_scratch_tables_expire() -> None:
    staging = RAW_EVENTS.with_name("_stg_x")
    scratch = type(staging)(
        staging.name, staging.columns, staging.partition_by, staging.cluster_by, 24
    )
    assert (
        "expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)"
        in render_ddl("p", "d", scratch)
    )


def test_audit_table_has_no_partition_clause() -> None:
    assert "PARTITION BY" not in render_ddl("p", "d", LOAD_AUDIT)


def test_duckdb_ddl_uses_local_types() -> None:
    ddl = duckdb_ddl("raw", RAW_EVENTS)
    assert '"raw"."raw_events"' in ddl and "VARCHAR" in ddl and "BIGINT" in ddl
    assert "PARTITION" not in ddl


def test_ref_and_statements_use_fully_qualified_names() -> None:
    fake = FakeBigQuery()
    bq = client(fake)
    assert bq.ref("raw_events") == "`my-proj.ecomm_raw.raw_events`"
    bq.ensure_table(RAW_EVENTS)
    bq.drop_table("_stg_x")
    assert fake.statements[0].startswith("CREATE TABLE IF NOT EXISTS")
    assert fake.statements[1] == "DROP TABLE IF EXISTS `my-proj.ecomm_raw._stg_x`"
    assert bq.query("SELECT 1") == [{"n": 3}]


def test_load_rows_uses_a_bulk_load_job_with_json_safe_values() -> None:
    fake = FakeBigQuery()
    client(fake).load_rows(
        LOAD_AUDIT.with_name("load_audit"),
        [{"started_at": datetime(2026, 11, 1, 10, 0, tzinfo=UTC), "d": date(2026, 11, 1)}],
    )
    rows, table = fake.loads[0]
    assert table == "my-proj.ecomm_raw.load_audit"
    assert rows[0]["started_at"] == "2026-11-01T10:00:00.000000Z"
    assert rows[0]["d"] == "2026-11-01"


def test_errors_are_wrapped_without_leaking_details() -> None:
    fake = FakeBigQuery()
    fake.fail = True
    with pytest.raises(WarehouseError) as exc:
        client(fake).execute("SELECT 1")
    assert "secret" not in str(exc.value)


def test_missing_configuration_is_an_actionable_error() -> None:
    with pytest.raises(WarehouseError, match="BIGQUERY_DATASET"):
        BigQueryClient("", "", client=object())


def test_identifiers_are_validated() -> None:
    assert safe_identifier("raw_events")
    for bad in ("a b", "a;drop", "1abc", "", 'x"y', "x`y"):
        with pytest.raises(ValueError, match="unsafe"):
            safe_identifier(bad)
