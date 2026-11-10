"""The Spark job's logic, tested without Spark and compared with the reference ingest."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from ecomm.generator import GeneratorConfig, generate
from ecomm.lake.commit import RetryPolicy
from ecomm.lake.ingest import classify
from ecomm.lake.manifest import manifest_key
from ecomm.lake.parity import reconcile_all
from ecomm.lake.sources import messages_from_emissions
from ecomm.lake.stats import list_ingest_stats
from ecomm.spark.dedup import dedup_step
from ecomm.spark.rows import STATUS_INVALID, STATUS_OK, classify_value, dedup_key
from ecomm.spark.sink import (
    BatchCommitError,
    BatchConflictError,
    BatchRow,
    SinkConfig,
    batch_id_for,
    process_batch,
)
from ecomm.storage.testing import FlakyObjectStore, InMemoryObjectStore

LATE = timedelta(minutes=60)
START = GeneratorConfig().start
CONFIG = GeneratorConfig(
    seed=17,
    orders=150,
    customers=25,
    end=START + timedelta(days=2),
    malformed_rate=0.05,
    late_rate=0.05,
    out_of_order_rate=0.10,
    duplicate_rate=0.08,
)


# --- helpers: emulate what Spark does around the logic -------------------------------------------


def rows_from_emissions() -> list[dict[str, Any]]:
    """Kafka-like rows with a classification verdict, in arrival order."""
    rows = []
    per_topic: dict[str, int] = defaultdict(int)
    for e in generate(CONFIG).emissions:
        offset = per_topic[e.topic]
        per_topic[e.topic] += 1
        value = e.value.encode()
        verdict = classify_value(value, e.arrival_time, LATE)
        rows.append(
            {
                "topic": e.topic,
                "partition": 0,
                "offset": offset,
                "key": e.key,
                "value": value,
                "ts_ms": int(e.arrival_time.timestamp() * 1000),
                "status": verdict.status,
                "event_id": verdict.event_id,
                "event_type": verdict.event_type,
                "event_time_iso": verdict.event_time_iso,
                "is_late": verdict.is_late,
                "error_code": verdict.error_code,
                "error_detail": verdict.error_detail,
            }
        )
    return rows


def run_stateful_dedup(
    micro_batches: list[list[dict[str, Any]]], state: dict[str, int], now_ms: int = 1
) -> list[list[BatchRow]]:
    """What applyInPandasWithState does: group by key per micro-batch, update state."""
    out: list[list[BatchRow]] = []
    for batch in micro_batches:
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in batch:
            groups[
                dedup_key(
                    row["status"], row["event_id"], row["topic"], row["partition"], row["offset"]
                )
            ].append(row)
        marked: list[BatchRow] = []
        for key, group in groups.items():
            result, new_state = dedup_step(group, state.get(key), now_ms)
            if new_state is not None:
                state[key] = new_state
            marked.extend(BatchRow(**r) for r in result)
        out.append(marked)
    return out


def sink() -> tuple[SinkConfig, InMemoryObjectStore, InMemoryObjectStore, InMemoryObjectStore]:
    s3, gcs, control = (
        InMemoryObjectStore("s3"),
        InMemoryObjectStore("gcs"),
        InMemoryObjectStore("ctl"),
    )
    cfg = SinkConfig("commerce", "spark-t", {"s3": s3, "gcs": gcs}, control, sleep=lambda _: None)
    return cfg, s3, gcs, control


def chunks(rows: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [rows[i : i + size] for i in range(0, len(rows), size)]


# --- classification --------------------------------------------------------------------------------


def test_classify_value_matches_the_reference_validator() -> None:
    emissions = generate(CONFIG).emissions
    oracle = classify(
        messages_from_emissions(emissions), set(), processed_at=START, late_threshold=LATE
    )
    verdicts = [classify_value(e.value.encode(), e.arrival_time, LATE) for e in emissions]
    assert sum(v.status == STATUS_INVALID for v in verdicts) == len(oracle.quarantined)
    assert (
        sum(v.is_late for v in verdicts if v.status == STATUS_OK) >= oracle.late_count
    )  # dups too


def test_null_and_binary_garbage_are_invalid_not_crashes() -> None:
    for value in (None, b"", b"\xff\xfe\x00", b"{"):
        verdict = classify_value(value, START, LATE)
        assert verdict.status == STATUS_INVALID and verdict.error_code == "INVALID_JSON"


def test_dedup_key_isolates_invalid_messages() -> None:
    assert dedup_key(STATUS_OK, "e1", "t", 0, 5) == "e1"
    assert dedup_key(STATUS_INVALID, None, "t", 0, 5) == "invalid:t:0:5"
    assert dedup_key(STATUS_INVALID, None, "t", 1, 5) != dedup_key(STATUS_INVALID, None, "t", 0, 5)


# --- dedup step ----------------------------------------------------------------------------------------


def row(partition: int, offset: int, status: str = STATUS_OK) -> dict[str, Any]:
    return {"partition": partition, "offset": offset, "status": status}


def test_first_copy_is_new_later_copies_are_duplicates() -> None:
    out, state = dedup_step([row(0, 9), row(0, 3), row(0, 5)], None, now_ms=100)
    assert [(r["offset"], r["is_duplicate"]) for r in out] == [(3, False), (5, True), (9, True)]
    assert state == 100


def test_state_from_an_earlier_batch_makes_everything_a_duplicate() -> None:
    out, state = dedup_step([row(0, 1)], 50, now_ms=200)
    assert out[0]["is_duplicate"] is True and state == 200  # last-seen refreshed


def test_invalid_rows_never_duplicate_and_never_touch_state() -> None:
    out, state = dedup_step([row(0, 1, STATUS_INVALID), row(0, 2, STATUS_INVALID)], None, now_ms=7)
    assert [r["is_duplicate"] for r in out] == [False, False] and state is None


# --- equivalence with the reference ingest -------------------------------------------------------------------


def test_micro_batched_stateful_processing_equals_the_reference_ingest() -> None:
    emissions = generate(CONFIG).emissions
    oracle = classify(
        messages_from_emissions(emissions), set(), processed_at=START, late_threshold=LATE
    )
    cfg, s3, gcs, control = sink()
    state: dict[str, int] = {}
    outcomes = []
    for number, batch in enumerate(run_stateful_dedup(chunks(rows_from_emissions(), 137), state)):
        outcomes.append(process_batch(batch, number, cfg))

    assert sum(o.valid for o in outcomes) == len(oracle.valid)
    assert sum(o.duplicates for o in outcomes) == len(oracle.duplicates)
    assert sum(o.quarantined for o in outcomes) == len(oracle.quarantined)
    assert sum(o.late for o in outcomes) == oracle.late_count
    assert sum(o.input_rows for o in outcomes) == oracle.input_count

    landed = {
        json.loads(line)["event"]["event_id"]
        for key in s3.list_objects("raw/")
        for line in s3.read_object(key).decode().splitlines()
    }
    assert landed == {str(e.envelope.event_id) for e in oracle.valid}
    assert s3.objects == gcs.objects  # both "clouds" hold identical bytes
    assert all(r.ok for r in reconcile_all({"s3": s3, "gcs": gcs}, "commerce"))


def test_ingest_statistics_are_recorded_per_batch_and_add_up() -> None:
    cfg, _, _, control = sink()
    for number, batch in enumerate(run_stateful_dedup(chunks(rows_from_emissions(), 200), {})):
        process_batch(batch, number, cfg)
    stats = list_ingest_stats(control, "commerce")
    assert len(stats) > 1
    assert sum(s.input_count for s in stats) == sum(
        s.valid + s.duplicates + s.quarantined for s in stats
    )


# --- idempotent replay (a restarted Spark query replays the last micro-batch) ------------------------------


def test_replaying_a_batch_changes_nothing() -> None:
    cfg, s3, gcs, control = sink()
    batches = run_stateful_dedup(chunks(rows_from_emissions(), 300), {})
    process_batch(batches[0], 0, cfg)
    before = (dict(s3.objects), dict(gcs.objects))
    stats_before = {k: v for k, v in control.objects.items() if k.startswith("ingest/")}

    again = process_batch(batches[0], 0, cfg)  # same micro-batch number and data
    assert (dict(s3.objects), dict(gcs.objects)) == before  # raw, manifests, quarantine identical
    assert {k: v for k, v in control.objects.items() if k.startswith("ingest/")} == stats_before
    ledger = json.loads(
        control.read_object(f"ledger/source=commerce/batch_id={again.batch_id}.json")
    )
    assert ledger["status"] == "COMPLETE"  # a replay does not downgrade the batch
    assert again.valid > 0


def test_restart_with_replay_equals_an_uninterrupted_run() -> None:
    all_batches = run_stateful_dedup(chunks(rows_from_emissions(), 150), {})
    clean_cfg, clean_s3, *_ = sink()
    for n, batch in enumerate(all_batches):
        process_batch(batch, n, clean_cfg)

    crash_cfg, crash_s3, *_ = sink()
    for n, batch in enumerate(all_batches[:3]):
        process_batch(batch, n, crash_cfg)
    process_batch(all_batches[2], 2, crash_cfg)  # replay of the batch that was in flight
    for n, batch in enumerate(all_batches[3:], start=3):
        process_batch(batch, n, crash_cfg)
    assert crash_s3.objects == clean_s3.objects


def test_checkpoint_reset_with_the_same_run_id_is_refused_not_overwritten() -> None:
    cfg, s3, *_ = sink()
    batches = run_stateful_dedup(chunks(rows_from_emissions(), 300), {})
    process_batch(batches[0], 0, cfg)
    snapshot = dict(s3.objects)
    with pytest.raises(BatchConflictError, match="new run_id"):
        process_batch(batches[1], 0, cfg)  # different data, same batch id
    assert s3.objects == snapshot  # committed data untouched


def test_partial_commit_raises_so_spark_retries_then_succeeds() -> None:
    s3 = InMemoryObjectStore("s3")
    gcs = FlakyObjectStore(
        InMemoryObjectStore("gcs"), fail_writes=10_000, fail_keys_containing="raw/"
    )
    control = InMemoryObjectStore("ctl")
    cfg = SinkConfig(
        "commerce",
        "spark-t",
        {"s3": s3, "gcs": gcs},
        control,  # type: ignore[dict-item]
        retry=RetryPolicy(max_attempts=2),
        sleep=lambda _: None,
    )
    batch = run_stateful_dedup(chunks(rows_from_emissions(), 400), {})[0]
    with pytest.raises(BatchCommitError, match="PARTIAL"):
        process_batch(batch, 0, cfg)
    assert s3.object_exists(
        manifest_key("commerce", batch_id_for("spark-t", 0))
    )  # healthy side kept

    gcs.fail_writes = 0  # outage over: Spark retries the same batch
    outcome = process_batch(batch, 0, cfg)
    assert outcome.committed and all(
        r.ok for r in reconcile_all({"s3": s3, "gcs": gcs}, "commerce")
    )  # type: ignore[dict-item]


def test_a_batch_with_only_invalid_rows_writes_quarantine_but_no_raw_batch() -> None:
    cfg, s3, _, control = sink()
    bad = [
        BatchRow(
            "commerce.orders",
            0,
            i,
            "k",
            b"{oops",
            1_000 + i,
            STATUS_INVALID,
            None,
            None,
            None,
            False,
            "INVALID_JSON",
            "JSONDecodeError",
            False,
        )
        for i in range(3)
    ]
    outcome = process_batch(bad, 4, cfg)
    assert not outcome.committed and outcome.quarantined == 3
    assert s3.list_objects("raw/") == [] and s3.list_objects("manifests/") == []
    [key] = s3.list_objects("quarantine/")
    assert len(s3.read_object(key).decode().splitlines()) == 3
    assert list_ingest_stats(control, "commerce")[0].quarantined == 3


def test_quarantine_only_conflict_is_also_refused() -> None:
    cfg, s3, *_ = sink()

    def bad(i: int, payload: bytes) -> BatchRow:
        return BatchRow(
            "commerce.orders",
            0,
            i,
            "k",
            payload,
            1_000 + i,
            STATUS_INVALID,
            None,
            None,
            None,
            False,
            "INVALID_JSON",
            "JSONDecodeError",
            False,
        )

    process_batch([bad(0, b"{a")], 7, cfg)
    snapshot = dict(s3.objects)
    with pytest.raises(BatchConflictError, match="quarantine"):
        process_batch([bad(0, b"{different")], 7, cfg)  # same batch id, different content
    assert s3.objects == snapshot


def test_quarantine_write_failure_is_retried_then_raised_for_spark_to_retry() -> None:
    s3 = InMemoryObjectStore("s3")
    gcs = FlakyObjectStore(InMemoryObjectStore("gcs"), fail_writes=10_000)
    sleeps: list[float] = []
    cfg = SinkConfig(
        "commerce",
        "spark-t",
        {"s3": s3, "gcs": gcs},
        InMemoryObjectStore("ctl"),  # type: ignore[dict-item]
        retry=RetryPolicy(max_attempts=3, base_delay_s=0.5),
        sleep=sleeps.append,
    )
    bad = BatchRow(
        "commerce.orders",
        0,
        0,
        "k",
        b"{a",
        1_000,
        STATUS_INVALID,
        None,
        None,
        None,
        False,
        "INVALID_JSON",
        "x",
        False,
    )
    with pytest.raises(BatchCommitError, match="quarantine write to gcs"):
        process_batch([bad], 0, cfg)
    assert sleeps == [0.5, 1.0]  # bounded exponential backoff, then give up


def test_empty_batch_is_a_noop() -> None:
    cfg, s3, _, control = sink()
    outcome = process_batch([], 0, cfg)
    assert outcome.input_rows == 0 and not s3.objects and not control.objects


def test_ingested_at_is_derived_from_the_data_so_replays_match() -> None:
    cfg, s3, *_ = sink()
    batch = run_stateful_dedup(chunks(rows_from_emissions(), 100), {})[0]
    process_batch(batch, 0, cfg)
    newest = max(r.ts_ms for r in batch)
    manifest = json.loads(s3.read_object(manifest_key("commerce", batch_id_for("spark-t", 0))))
    expected = datetime.fromtimestamp(newest / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%S")
    assert manifest["ingested_at"].startswith(expected)
