from __future__ import annotations

import json

from lake_helpers import LATE_THRESHOLD, PROCESSED_AT

from ecomm.generator import FaultKind
from ecomm.lake.ingest import ClassifiedBatch, SourceMessage, classify, write_quarantine
from ecomm.storage.testing import InMemoryObjectStore


def run(messages: list[SourceMessage], seen: set[str]) -> ClassifiedBatch:
    return classify(messages, seen, processed_at=PROCESSED_AT, late_threshold=LATE_THRESHOLD)


def test_counts_match_ground_truth_labels(
    classified: ClassifiedBatch, fault_counts: dict[FaultKind, int], messages: list[SourceMessage]
) -> None:
    assert len(classified.quarantined) == fault_counts[FaultKind.MALFORMED]
    assert len(classified.duplicates) == fault_counts[FaultKind.DUPLICATE]
    assert classified.late_count == fault_counts[FaultKind.LATE]
    assert classified.input_count == len(messages)


def test_valid_events_are_unique_by_event_id(classified: ClassifiedBatch) -> None:
    ids = [str(e.envelope.event_id) for e in classified.valid]
    assert len(ids) == len(set(ids))


def test_duplicates_were_all_seen_before(classified: ClassifiedBatch) -> None:
    valid_ids = {str(e.envelope.event_id) for e in classified.valid}
    assert all(str(d.envelope.event_id) in valid_ids for d in classified.duplicates)


def test_quarantine_keeps_reason_and_original_payload(classified: ClassifiedBatch) -> None:
    assert classified.quarantined
    for record in classified.quarantined:
        assert record.error_code
        assert record.original_payload
        assert record.topic.startswith("commerce.")
        assert record.offset >= 0
        assert record.processed_at == PROCESSED_AT


def test_dedup_state_carries_across_micro_batches(messages: list[SourceMessage]) -> None:
    seen: set[str] = set()
    half = len(messages) // 2
    first = run(messages[:half], seen)
    second = run(messages[half:], seen)
    whole = run(messages, set())
    assert len(first.valid) + len(second.valid) == len(whole.valid)


def test_replaying_the_same_messages_adds_no_new_valid_events(
    messages: list[SourceMessage],
) -> None:
    seen: set[str] = set()
    run(messages, seen)
    assert run(messages, seen).valid == []


def test_write_quarantine(classified: ClassifiedBatch) -> None:
    store = InMemoryObjectStore()
    key = write_quarantine(store, "commerce", "b1", classified.quarantined)
    assert key == "quarantine/source=commerce/batch_id=b1/part-0.jsonl"
    assert key is not None
    rows = [json.loads(line) for line in store.read_object(key).decode().splitlines()]
    assert len(rows) == len(classified.quarantined)
    assert {"error_code", "error_detail", "original_payload", "processed_at"} <= set(rows[0])
    assert write_quarantine(store, "commerce", "b2", []) is None
