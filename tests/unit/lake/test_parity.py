from __future__ import annotations

from lake_helpers import PROCESSED_AT

from ecomm.lake.commit import commit_batch
from ecomm.lake.ingest import ClassifiedBatch
from ecomm.lake.manifest import manifest_key
from ecomm.lake.parity import ParityStatus, list_batch_ids, reconcile_all, reconcile_batch
from ecomm.lake.raw import PreparedBatch, prepare_batch
from ecomm.storage.testing import InMemoryObjectStore

BATCH = "run-001-b00000"


def committed(prepared: PreparedBatch) -> tuple[InMemoryObjectStore, InMemoryObjectStore]:
    s3, gcs = InMemoryObjectStore("s3"), InMemoryObjectStore("gcs")
    commit_batch(prepared, {"s3": s3, "gcs": gcs}, sleep=lambda _: None)
    return s3, gcs


def test_matching_clouds(prepared: PreparedBatch) -> None:
    s3, gcs = committed(prepared)
    report = reconcile_batch({"s3": s3, "gcs": gcs}, "commerce", BATCH)
    assert report.status is ParityStatus.MATCH
    assert report.destinations["s3"].event_count == prepared.manifest.event_count
    assert report.destinations["s3"].logical_checksum == prepared.manifest.logical_checksum


def test_tampered_object_is_detected(prepared: PreparedBatch) -> None:
    s3, gcs = committed(prepared)
    key = next(iter(prepared.objects))
    assert b'"ingest"' in gcs.objects[key]
    gcs.objects[key] = gcs.objects[key].replace(b'"ingest"', b'"INGEST"', 1)
    report = reconcile_batch({"s3": s3, "gcs": gcs}, "commerce", BATCH)
    assert not report.ok
    assert any("checksum mismatch" in p and "gcs" in p for p in report.problems)


def test_dropped_record_is_detected_at_logical_level(prepared: PreparedBatch) -> None:
    s3, gcs = committed(prepared)
    key = next(iter(prepared.objects))
    lines = gcs.objects[key].splitlines(keepends=True)
    gcs.objects[key] = b"".join(lines[1:])  # one record lost
    report = reconcile_batch({"s3": s3, "gcs": gcs}, "commerce", BATCH)
    assert not report.ok
    assert any("count" in p for p in report.problems)


def test_missing_object_is_detected(prepared: PreparedBatch) -> None:
    s3, gcs = committed(prepared)
    del gcs.objects[next(iter(prepared.objects))]
    report = reconcile_batch({"s3": s3, "gcs": gcs}, "commerce", BATCH)
    assert any("object missing" in p for p in report.problems)


def test_missing_manifest_in_one_cloud(prepared: PreparedBatch) -> None:
    s3, gcs = committed(prepared)
    del gcs.objects[manifest_key("commerce", BATCH)]
    report = reconcile_batch({"s3": s3, "gcs": gcs}, "commerce", BATCH)
    assert report.status is ParityStatus.MISMATCH
    assert not report.destinations["gcs"].manifest_found


def test_reconcile_all_covers_batches_present_in_only_one_cloud(
    classified: ClassifiedBatch, prepared: PreparedBatch
) -> None:
    s3, gcs = committed(prepared)
    second = prepare_batch(
        classified.valid[:5],
        source="commerce",
        run_id="run-002",
        batch_id="run-002-b00000",
        ingested_at=PROCESSED_AT,
    )
    commit_batch(second, {"s3": s3}, sleep=lambda _: None)  # never reached gcs

    assert list_batch_ids(s3, "commerce") == [BATCH, "run-002-b00000"]
    reports = {r.batch_id: r for r in reconcile_all({"s3": s3, "gcs": gcs}, "commerce")}
    assert reports[BATCH].ok
    assert not reports["run-002-b00000"].ok
