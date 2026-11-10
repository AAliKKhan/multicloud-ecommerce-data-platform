"""Shared builders for tests that need a populated raw lake."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from ecomm.generator import GeneratorConfig, generate
from ecomm.lake.commit import BatchStatus, commit_batch
from ecomm.lake.ingest import ClassifiedBatch, IngestedEvent, classify
from ecomm.lake.raw import PreparedBatch, prepare_batch
from ecomm.lake.sources import messages_from_emissions
from ecomm.storage.interface import ObjectStore

SOURCE = "commerce"
PROCESSED_AT = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)
LATE_THRESHOLD = timedelta(minutes=60)

THREE_DAYS = GeneratorConfig(
    seed=5,
    orders=300,
    customers=40,
    end=datetime(2026, 11, 1, tzinfo=UTC) + timedelta(days=3),
    malformed_rate=0.03,
    late_rate=0.03,
    out_of_order_rate=0.05,
    duplicate_rate=0.04,
)


def classify_config(config: GeneratorConfig) -> ClassifiedBatch:
    messages = messages_from_emissions(generate(config).emissions)
    return classify(messages, set(), processed_at=PROCESSED_AT, late_threshold=LATE_THRESHOLD)


def split_by_time(events: list[IngestedEvent], parts: int) -> list[list[IngestedEvent]]:
    ordered = sorted(events, key=lambda e: (e.envelope.event_time, str(e.envelope.event_id)))
    size = -(-len(ordered) // parts)
    return [ordered[i : i + size] for i in range(0, len(ordered), size)]


def commit_batches(
    batches: list[list[IngestedEvent]], store: ObjectStore, run_id: str = "run"
) -> list[PreparedBatch]:
    prepared = []
    for index, events in enumerate(batches):
        batch = prepare_batch(
            events,
            source=SOURCE,
            run_id=run_id,
            batch_id=f"{run_id}-b{index:05d}",
            ingested_at=PROCESSED_AT,
        )
        result = commit_batch(batch, {store.name: store}, sleep=lambda _: None)
        assert result.status is BatchStatus.COMPLETE
        prepared.append(batch)
    return prepared
