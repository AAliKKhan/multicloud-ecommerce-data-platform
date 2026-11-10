from __future__ import annotations

import json
from datetime import date
from typing import Any

import pytest

from ecomm.lake.ingest import IngestedEvent
from ecomm.lake.manifest import manifest_key
from ecomm.lake.raw import PreparedBatch
from ecomm.storage.testing import InMemoryObjectStore
from ecomm.warehouse.client import WarehouseError
from ecomm.warehouse.duckdb_client import DuckDBClient
from ecomm.warehouse.loader import LoadStatus, WarehouseLoader
from ecomm.warehouse.schema import LOAD_AUDIT, RAW_EVENTS
from support import SOURCE, THREE_DAYS, classify_config, commit_batches, split_by_time


@pytest.fixture(scope="module")
def valid_events() -> list[IngestedEvent]:
    return classify_config(THREE_DAYS).valid


@pytest.fixture()
def lake(valid_events: list[IngestedEvent]) -> tuple[InMemoryObjectStore, list[PreparedBatch]]:
    store = InMemoryObjectStore("s3")
    return store, commit_batches(split_by_time(valid_events, 3), store)


@pytest.fixture()
def client() -> DuckDBClient:
    return DuckDBClient()


def count(client: DuckDBClient, table: str = RAW_EVENTS.name, where: str = "TRUE") -> int:
    return int(client.query(f"SELECT COUNT(*) AS n FROM {client.ref(table)} WHERE {where}")[0]["n"])


def order_total(client: DuckDBClient) -> float:
    rows = client.query(
        f"SELECT SUM(CAST(json_extract_string(payload, '$.total_amount') AS DECIMAL(18,2))) AS s "
        f"FROM {client.ref('raw_events')} WHERE event_type = 'order_created'"
    )
    return float(rows[0]["s"])


def test_load_batch_inserts_every_event_with_typed_columns(
    client: DuckDBClient, lake: tuple[InMemoryObjectStore, list[PreparedBatch]]
) -> None:
    store, batches = lake
    loader = WarehouseLoader(client, store, SOURCE)
    result = loader.load_batch(batches[0].manifest)

    manifest = batches[0].manifest
    assert result.status is LoadStatus.SUCCEEDED
    assert (result.rows_read, result.rows_inserted, result.rows_duplicate) == (
        manifest.event_count,
        manifest.event_count,
        0,
    )
    assert count(client) == manifest.event_count
    row: dict[str, Any] = client.query(f"SELECT * FROM {client.ref('raw_events')} LIMIT 1")[0]
    assert row["event_date"] == row["event_time"].date()
    assert row["load_id"] == f"load-{manifest.batch_id}"
    assert json.loads(row["payload"])  # JSON text
    audit = client.query(f"SELECT * FROM {client.ref(LOAD_AUDIT.name)}")
    assert len(audit) == 1 and audit[0]["status"] == "SUCCEEDED"
    assert audit[0]["expected_rows"] == audit[0]["rows_read"] == manifest.event_count


def test_replaying_a_batch_does_not_change_totals(
    client: DuckDBClient, lake: tuple[InMemoryObjectStore, list[PreparedBatch]]
) -> None:
    store, batches = lake
    loader = WarehouseLoader(client, store, SOURCE)
    manifest = batches[0].manifest
    loader.load_batch(manifest)
    rows_before, total_before = count(client), order_total(client)

    for _ in range(3):
        replay = loader.load_batch(manifest)
        assert replay.status is LoadStatus.SUCCEEDED
        assert replay.rows_inserted == 0
        assert replay.rows_duplicate == manifest.event_count

    assert count(client) == rows_before
    assert order_total(client) == total_before
    assert count(client, LOAD_AUDIT.name) == 4  # every attempt is audited


def test_event_ids_stay_unique_across_overlapping_batches(
    client: DuckDBClient, valid_events: list[IngestedEvent]
) -> None:
    store = InMemoryObjectStore("s3")
    ordered = split_by_time(valid_events, 1)[0]
    overlap = [ordered[:60], ordered[30:90]]  # 30 events appear in both batches
    batches = commit_batches(overlap, store)
    loader = WarehouseLoader(client, store, SOURCE)
    results = [loader.load_batch(b.manifest) for b in batches]
    assert [r.rows_duplicate for r in results] == [0, 30]
    assert count(client) == 90
    assert (
        int(
            client.query(f"SELECT COUNT(DISTINCT event_id) AS n FROM {client.ref('raw_events')}")[
                0
            ]["n"]
        )
        == 90
    )


def test_corrupted_source_object_fails_without_loading_anything(
    client: DuckDBClient, lake: tuple[InMemoryObjectStore, list[PreparedBatch]]
) -> None:
    store, batches = lake
    key = batches[0].manifest.objects[0].key
    store.objects[key] = store.objects[key] + b" "
    result = WarehouseLoader(client, store, SOURCE).load_batch(batches[0].manifest)
    assert result.status is LoadStatus.FAILED
    assert "checksum" in (result.error or "")
    assert count(client) == 0
    assert count(client, LOAD_AUDIT.name, "status = 'FAILED'") == 1
    leftovers = client.query(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'raw' AND table_name LIKE '\\_stg%' ESCAPE '\\'"
    )
    assert leftovers == []  # scratch staging table was cleaned up


def test_source_to_target_count_mismatch_is_rejected(
    client: DuckDBClient, lake: tuple[InMemoryObjectStore, list[PreparedBatch]]
) -> None:
    store, batches = lake
    lying = batches[0].manifest.model_copy(
        update={"event_count": batches[0].manifest.event_count + 1}
    )
    result = WarehouseLoader(client, store, SOURCE).load_batch(lying)
    assert result.status is LoadStatus.FAILED
    assert "manifest event_count" in (result.error or "")
    assert count(client) == 0


class FailingMergeClient(DuckDBClient):
    fail_merge = True

    def execute(self, sql: str) -> None:
        if self.fail_merge and sql.startswith("MERGE"):
            raise WarehouseError("simulated warehouse outage")
        super().execute(sql)


def test_failed_merge_leaves_target_untouched_and_retry_succeeds(
    lake: tuple[InMemoryObjectStore, list[PreparedBatch]],
) -> None:
    store, batches = lake
    client = FailingMergeClient()
    loader = WarehouseLoader(client, store, SOURCE)
    failed = loader.load_batch(batches[0].manifest)
    assert failed.status is LoadStatus.FAILED
    assert count(client) == 0

    client.fail_merge = False
    retried = loader.load_batch(batches[0].manifest)
    assert retried.status is LoadStatus.SUCCEEDED
    assert count(client) == batches[0].manifest.event_count
    assert loader.latest_status(batches[0].manifest.batch_id) is LoadStatus.SUCCEEDED


# --- backfill ---------------------------------------------------------------------------------


def test_backfill_loads_all_batches_and_is_idempotent(
    client: DuckDBClient, lake: tuple[InMemoryObjectStore, list[PreparedBatch]]
) -> None:
    store, batches = lake
    loader = WarehouseLoader(client, store, SOURCE)
    first = loader.backfill(date(2026, 11, 1), date(2026, 11, 3), "bf-1")
    total = sum(b.manifest.event_count for b in batches)
    assert first.ok and len(first.results) == 3
    assert sum(r.rows_inserted for r in first.results) == total
    assert count(client) == total

    second = loader.backfill(date(2026, 11, 1), date(2026, 11, 3), "bf-2")
    assert second.ok
    assert sum(r.rows_inserted for r in second.results) == 0
    assert count(client) == total


def test_backfill_respects_date_range_and_skip_succeeded(
    client: DuckDBClient, lake: tuple[InMemoryObjectStore, list[PreparedBatch]]
) -> None:
    store, batches = lake
    loader = WarehouseLoader(client, store, SOURCE)
    first_day = loader.backfill(date(2026, 11, 1), date(2026, 11, 1), "bf-day1")
    assert 1 <= len(first_day.results) < 3
    selected = {r.batch_id for r in first_day.results}

    everything = loader.backfill(
        date(2026, 11, 1), date(2026, 11, 3), "bf-all", skip_succeeded=True
    )
    assert set(everything.skipped) == selected
    assert len(everything.results) + len(everything.skipped) == 3
    assert count(client) == sum(b.manifest.event_count for b in batches)

    nothing = loader.backfill(date(2027, 1, 1), date(2027, 1, 2), "bf-none")
    assert nothing.results == [] and nothing.ok


def test_backfill_reports_failures_but_still_loads_healthy_batches(
    client: DuckDBClient, lake: tuple[InMemoryObjectStore, list[PreparedBatch]]
) -> None:
    store, batches = lake
    bad_key = batches[1].manifest.objects[0].key
    store.objects[bad_key] = b"garbage\n"
    report = WarehouseLoader(client, store, SOURCE).backfill(
        date(2026, 11, 1), date(2026, 11, 3), "bf-bad"
    )
    statuses = {r.batch_id: r.status for r in report.results}
    assert not report.ok
    assert statuses[batches[1].manifest.batch_id] is LoadStatus.FAILED
    assert statuses[batches[0].manifest.batch_id] is LoadStatus.SUCCEEDED
    assert statuses[batches[2].manifest.batch_id] is LoadStatus.SUCCEEDED


def test_backfill_report_is_written_and_rejects_bad_ranges(
    client: DuckDBClient, lake: tuple[InMemoryObjectStore, list[PreparedBatch]]
) -> None:
    store, _ = lake
    loader = WarehouseLoader(client, store, SOURCE)
    report = loader.backfill(date(2026, 11, 1), date(2026, 11, 3), "bf-report")
    control = InMemoryObjectStore("control")
    saved = json.loads(control.read_object(report.write(control)))
    assert saved["ok"] is True and saved["batches_loaded"] == 3
    assert saved["rows_inserted"] == saved["rows_read"]
    with pytest.raises(ValueError, match="end must not be before start"):
        loader.backfill(date(2026, 11, 3), date(2026, 11, 1), "bf-bad-range")


def test_only_committed_batches_are_loaded(
    client: DuckDBClient, lake: tuple[InMemoryObjectStore, list[PreparedBatch]]
) -> None:
    store, batches = lake
    del store.objects[manifest_key(SOURCE, batches[2].manifest.batch_id)]  # not committed
    report = WarehouseLoader(client, store, SOURCE).backfill(
        date(2026, 11, 1), date(2026, 11, 3), "bf-c"
    )
    assert {r.batch_id for r in report.results} == {b.manifest.batch_id for b in batches[:2]}
