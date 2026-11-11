from __future__ import annotations

from lake_helpers import PROCESSED_AT

from ecomm.lake.commit import commit_batch
from ecomm.lake.ingest import ClassifiedBatch
from ecomm.lake.manifest import BatchManifest, manifest_key
from ecomm.lake.parity import (
    ParityStatus,
    list_batch_ids,
    reconcile_all,
    reconcile_all_light,
    reconcile_batch,
)
from ecomm.lake.raw import PreparedBatch, prepare_batch
from ecomm.storage.testing import InMemoryObjectStore

BATCH = "run-001-b00000"


def committed(prepared: PreparedBatch) -> tuple[InMemoryObjectStore, InMemoryObjectStore]:
    s3, gcs = InMemoryObjectStore("s3"), InMemoryObjectStore("gcs")
    commit_batch(prepared, {"s3": s3, "gcs": gcs}, sleep=lambda _: None)
    return s3, gcs


# --- the light check used by the metrics exporter (manifests + object presence only) ------------


def light(s3: InMemoryObjectStore, gcs: InMemoryObjectStore):  # type: ignore[no-untyped-def]
    return reconcile_all_light({"s3": s3, "gcs": gcs}, "commerce")


def test_light_check_matches_on_a_healthy_lake_like_the_full_check(prepared: PreparedBatch) -> None:
    s3, gcs = committed(prepared)
    [report] = light(s3, gcs)
    assert report.ok and report.batch_id == BATCH
    assert report.destinations["s3"].event_count == prepared.manifest.event_count


def test_light_check_reads_no_data_objects(prepared: PreparedBatch) -> None:
    s3, gcs = committed(prepared)
    reads: list[str] = []
    original = s3.read_object

    def spy(key: str) -> bytes:
        reads.append(key)
        return original(key)

    s3.read_object = spy  # type: ignore[method-assign]
    light(s3, gcs)
    assert reads and all(key.startswith("manifests/") for key in reads)  # never raw/ objects


def test_light_check_detects_a_missing_manifest_a_missing_object_and_divergent_manifests(
    prepared: PreparedBatch, classified: ClassifiedBatch
) -> None:
    s3, gcs = committed(prepared)
    del gcs.objects[manifest_key("commerce", BATCH)]
    [no_manifest] = light(s3, gcs)
    assert not no_manifest.ok and "manifest missing" in " ".join(no_manifest.problems)

    s3, gcs = committed(prepared)
    del gcs.objects[next(iter(prepared.objects))]
    [no_object] = light(s3, gcs)
    assert not no_object.ok and "object missing" in " ".join(no_object.problems)

    s3, gcs = committed(prepared)
    other = BatchManifest.from_bytes(gcs.objects[manifest_key("commerce", BATCH)])
    other = other.model_copy(update={"event_count": other.event_count + 1})
    gcs.objects[manifest_key("commerce", BATCH)] = other.to_bytes()
    [divergent] = light(s3, gcs)
    assert not divergent.ok and "different manifests" in " ".join(divergent.problems)


def test_light_check_covers_batches_that_exist_in_only_one_destination(
    prepared: PreparedBatch, classified: ClassifiedBatch
) -> None:
    s3, gcs = committed(prepared)
    second = prepare_batch(
        classified.valid[:5], source="commerce", run_id="run-002", batch_id="run-002-b00000",
        ingested_at=PROCESSED_AT,
    )  # fmt: skip
    commit_batch(second, {"s3": s3}, sleep=lambda _: None)
    reports = {r.batch_id: r for r in light(s3, gcs)}
    assert reports[BATCH].ok and not reports["run-002-b00000"].ok


def test_light_check_does_not_see_altered_bytes_but_the_full_check_does(
    prepared: PreparedBatch,
) -> None:
    """The documented trade-off: cheap enough for every refresh, but not byte-level."""
    s3, gcs = committed(prepared)
    key = next(iter(prepared.objects))
    gcs.objects[key] = gcs.objects[key].replace(b'"ingest"', b'"INGEST"', 1)
    assert light(s3, gcs)[0].ok
    assert not reconcile_batch({"s3": s3, "gcs": gcs}, "commerce", BATCH).ok


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
