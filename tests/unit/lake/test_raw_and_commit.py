from __future__ import annotations

import json
import re
from collections.abc import Mapping

import pytest
from lake_helpers import PROCESSED_AT

from ecomm.lake.commit import (
    BatchStatus,
    DestinationState,
    RetryPolicy,
    commit_batch,
    ledger_key,
    record_status,
)
from ecomm.lake.ingest import ClassifiedBatch
from ecomm.lake.manifest import BatchManifest, manifest_key
from ecomm.lake.parity import reconcile_batch
from ecomm.lake.raw import PreparedBatch, prepare_batch
from ecomm.storage.interface import ObjectStore
from ecomm.storage.testing import FlakyObjectStore, InMemoryObjectStore

KEY_RE = re.compile(
    r"^raw/commerce/[a-z_]+/event_date=\d{4}-\d{2}-\d{2}/event_hour=\d{2}/part-run-001-b00000\.jsonl$"
)
NO_SLEEP: list[float] = []


def sleeper(delay: float) -> None:
    NO_SLEEP.append(delay)


def stores() -> tuple[InMemoryObjectStore, InMemoryObjectStore]:
    return InMemoryObjectStore("s3"), InMemoryObjectStore("gcs")


# --- raw batch ------------------------------------------------------------------


def test_prepare_batch_is_deterministic(
    classified: ClassifiedBatch, prepared: PreparedBatch
) -> None:
    again = prepare_batch(
        list(reversed(classified.valid)),  # input order must not matter
        source="commerce",
        run_id="run-001",
        batch_id="run-001-b00000",
        ingested_at=PROCESSED_AT,
    )
    assert again.objects == prepared.objects
    assert again.manifest == prepared.manifest


def test_keys_follow_the_documented_layout(prepared: PreparedBatch) -> None:
    assert prepared.objects
    assert all(KEY_RE.match(key) for key in prepared.objects)


def test_manifest_describes_the_objects(
    classified: ClassifiedBatch, prepared: PreparedBatch
) -> None:
    manifest = prepared.manifest
    assert manifest.event_count == len(classified.valid)
    assert sum(o.record_count for o in manifest.objects) == manifest.event_count
    assert {o.key for o in manifest.objects} == set(prepared.objects)
    assert manifest.min_event_time is not None and manifest.max_event_time is not None
    assert manifest.min_event_time <= manifest.max_event_time
    assert manifest.source_offsets
    assert BatchManifest.from_bytes(manifest.to_bytes()) == manifest


def test_lines_keep_the_original_event_and_add_lineage(
    classified: ClassifiedBatch, prepared: PreparedBatch
) -> None:
    originals = {str(e.envelope.event_id): json.loads(e.message.value) for e in classified.valid}
    seen = 0
    for key, data in prepared.objects.items():
        for line in data.decode().splitlines():
            row = json.loads(line)
            event = row["event"]
            assert event == originals[event["event_id"]]  # immutable raw payload
            assert key.split("/")[2] == event["event_type"]
            assert event["event_time"][:10] in key  # partitioned by event date
            ingest = row["ingest"]
            assert ingest["batch_id"] == "run-001-b00000"
            assert {"source_topic", "kafka_partition", "kafka_offset", "is_late"} <= set(ingest)
            seen += 1
    assert seen == len(classified.valid)


def test_empty_and_duplicate_batches_are_rejected(classified: ClassifiedBatch) -> None:
    kwargs = {"source": "commerce", "run_id": "r", "batch_id": "b", "ingested_at": PROCESSED_AT}
    with pytest.raises(ValueError, match="empty"):
        prepare_batch([], **kwargs)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="duplicate"):
        prepare_batch([classified.valid[0], classified.valid[0]], **kwargs)  # type: ignore[arg-type]


# --- dual commit -------------------------------------------------------------------


def test_commit_to_both_clouds_is_complete_and_matches(prepared: PreparedBatch) -> None:
    s3, gcs = stores()
    result = commit_batch(prepared, {"s3": s3, "gcs": gcs}, sleep=sleeper)
    assert result.status is BatchStatus.COMPLETE
    for store in (s3, gcs):
        assert store.object_exists(manifest_key("commerce", "run-001-b00000"))
        assert set(prepared.objects) <= set(store.objects)
    assert s3.objects == gcs.objects  # same logical (and byte) content
    assert reconcile_batch({"s3": s3, "gcs": gcs}, "commerce", "run-001-b00000").ok


def test_rerunning_a_committed_batch_writes_nothing(prepared: PreparedBatch) -> None:
    s3, gcs = (
        FlakyObjectStore(InMemoryObjectStore("s3")),
        FlakyObjectStore(InMemoryObjectStore("gcs")),
    )
    commit_batch(prepared, {"s3": s3, "gcs": gcs}, sleep=sleeper)
    before = {
        name: sum(store.write_attempts.values()) for name, store in (("s3", s3), ("gcs", gcs))
    }
    again = commit_batch(prepared, {"s3": s3, "gcs": gcs}, sleep=sleeper)
    assert again.status is BatchStatus.COMPLETE
    assert all(r.already_committed and r.attempts == 0 for r in again.destinations.values())
    after = {name: sum(store.write_attempts.values()) for name, store in (("s3", s3), ("gcs", gcs))}
    assert before == after


def test_transient_failure_is_retried_with_bounded_backoff(prepared: PreparedBatch) -> None:
    NO_SLEEP.clear()
    s3 = InMemoryObjectStore("s3")
    gcs = FlakyObjectStore(InMemoryObjectStore("gcs"), fail_writes=2)
    result = commit_batch(
        prepared, {"s3": s3, "gcs": gcs}, retry=RetryPolicy(4, 0.5, 8.0), sleep=sleeper
    )
    assert result.status is BatchStatus.COMPLETE
    assert result.destinations["gcs"].attempts == 3
    assert NO_SLEEP == [0.5, 1.0]
    assert reconcile_batch({"s3": s3, "gcs": gcs}, "commerce", "run-001-b00000").ok


def test_backoff_is_capped() -> None:
    policy = RetryPolicy(max_attempts=10, base_delay_s=1, max_delay_s=5)
    assert [policy.delay(n) for n in range(1, 6)] == [1, 2, 4, 5, 5]


def test_one_cloud_down_gives_partial_then_retry_completes_only_the_failed_one(
    prepared: PreparedBatch,
) -> None:
    s3 = FlakyObjectStore(InMemoryObjectStore("s3"))
    gcs = FlakyObjectStore(InMemoryObjectStore("gcs"), fail_writes=10_000)
    result = commit_batch(prepared, {"s3": s3, "gcs": gcs}, retry=RetryPolicy(3), sleep=sleeper)

    assert result.status is BatchStatus.PARTIAL
    assert result.destinations["s3"].state is DestinationState.COMMITTED
    assert result.destinations["gcs"].state is DestinationState.FAILED
    assert result.destinations["gcs"].attempts == 3
    assert not gcs.object_exists(manifest_key("commerce", "run-001-b00000"))  # not committed
    assert not reconcile_batch({"s3": s3, "gcs": gcs}, "commerce", "run-001-b00000").ok

    s3_writes_before = sum(s3.write_attempts.values())
    gcs.fail_writes = 0  # outage over
    retry = commit_batch(prepared, {"s3": s3, "gcs": gcs}, sleep=sleeper)
    assert retry.status is BatchStatus.COMPLETE
    assert retry.destinations["s3"].already_committed
    assert sum(s3.write_attempts.values()) == s3_writes_before  # healthy side untouched
    assert reconcile_batch({"s3": s3, "gcs": gcs}, "commerce", "run-001-b00000").ok


def test_a_destination_that_cannot_even_be_read_is_partial_not_a_crash(
    prepared: PreparedBatch,
) -> None:
    """A real outage fails reads too. Found with MinIO: the manifest pre-check used to raise out of
    ``commit_batch``, so no status was returned and the healthy destination was never written."""
    s3 = FlakyObjectStore(InMemoryObjectStore("s3"))
    gcs = FlakyObjectStore(InMemoryObjectStore("gcs"), fail_writes=10_000, fail_reads=10_000)
    # The broken destination comes first, so a crash would also skip the healthy one.
    result = commit_batch(prepared, {"gcs": gcs, "s3": s3}, retry=RetryPolicy(2), sleep=sleeper)

    assert result.status is BatchStatus.PARTIAL
    assert result.destinations["s3"].state is DestinationState.COMMITTED
    assert result.destinations["gcs"].state is DestinationState.FAILED
    assert (
        result.destinations["gcs"].error and "simulated outage" in result.destinations["gcs"].error
    )

    gcs.fail_writes = gcs.fail_reads = 0  # outage over: the retry completes only the failed side
    healed = commit_batch(prepared, {"gcs": gcs, "s3": s3}, sleep=sleeper)
    assert healed.status is BatchStatus.COMPLETE
    assert healed.destinations["s3"].already_committed


def test_an_unreadable_manifest_on_a_committed_batch_just_rewrites_the_same_bytes(
    prepared: PreparedBatch,
) -> None:
    s3 = FlakyObjectStore(InMemoryObjectStore("s3"))
    assert commit_batch(prepared, {"s3": s3}, sleep=sleeper).status is BatchStatus.COMPLETE
    before = {k: s3.inner.read_object(k) for k in s3.inner.list_objects()}

    s3.fail_reads = 1  # one transient read failure on the 'already committed?' check
    again = commit_batch(prepared, {"s3": s3}, sleep=sleeper)
    assert again.status is BatchStatus.COMPLETE
    assert {k: s3.inner.read_object(k) for k in s3.inner.list_objects()} == before  # idempotent


def test_all_destinations_down_is_failed(prepared: PreparedBatch) -> None:
    down = {
        "s3": FlakyObjectStore(InMemoryObjectStore("s3"), fail_writes=10_000),
        "gcs": FlakyObjectStore(InMemoryObjectStore("gcs"), fail_writes=10_000),
    }
    result = commit_batch(prepared, down, retry=RetryPolicy(2), sleep=sleeper)  # type: ignore[arg-type]
    assert result.status is BatchStatus.FAILED
    assert all(r.error for r in result.destinations.values())


def test_manifest_is_written_last_so_failed_manifest_means_uncommitted(
    prepared: PreparedBatch,
) -> None:
    gcs = FlakyObjectStore(
        InMemoryObjectStore("gcs"), fail_writes=10_000, fail_keys_containing="manifest.json"
    )
    result = commit_batch(prepared, {"gcs": gcs}, retry=RetryPolicy(2), sleep=sleeper)
    assert result.status is BatchStatus.FAILED
    assert set(prepared.objects) <= set(gcs.inner.list_objects("raw/"))  # data landed...
    assert not gcs.object_exists(manifest_key("commerce", "run-001-b00000"))  # ...but not committed


def test_optional_destination_failure_does_not_block_completion(prepared: PreparedBatch) -> None:
    s3 = InMemoryObjectStore("s3")
    gcs = FlakyObjectStore(InMemoryObjectStore("gcs"), fail_writes=10_000)
    result = commit_batch(
        prepared, {"s3": s3, "gcs": gcs}, required=["s3"], retry=RetryPolicy(2), sleep=sleeper
    )
    assert result.status is BatchStatus.COMPLETE
    assert result.destinations["gcs"].state is DestinationState.FAILED


def test_unknown_required_destination_is_a_programming_error(prepared: PreparedBatch) -> None:
    with pytest.raises(ValueError, match="not configured"):
        commit_batch(prepared, {"s3": InMemoryObjectStore()}, required=["gcs"])


class CorruptingStore(InMemoryObjectStore):
    """Silently stores damaged bytes: read-back verification must catch it."""

    def write_object(
        self, key: str, data: bytes, metadata: Mapping[str, str] | None = None
    ) -> None:
        super().write_object(key, data[:-1] if key.startswith("raw/") else data, metadata)


def test_read_back_verification_detects_silent_corruption(prepared: PreparedBatch) -> None:
    bad: ObjectStore = CorruptingStore("gcs")
    result = commit_batch(prepared, {"gcs": bad}, retry=RetryPolicy(2), sleep=sleeper)
    assert result.status is BatchStatus.FAILED
    assert "checksum" in (result.destinations["gcs"].error or "")
    assert not bad.object_exists(manifest_key("commerce", "run-001-b00000"))


def test_status_is_recorded_in_the_ledger(prepared: PreparedBatch) -> None:
    control = InMemoryObjectStore("control")
    result = commit_batch(prepared, {"s3": InMemoryObjectStore()}, sleep=sleeper)
    record_status(control, result)
    saved = json.loads(control.read_object(ledger_key("commerce", "run-001-b00000")))
    assert saved["status"] == "COMPLETE"
    assert saved["destinations"]["s3"]["state"] == "COMMITTED"
