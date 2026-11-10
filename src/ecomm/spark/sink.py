"""Micro-batch sink: classified rows -> raw files, quarantine, ledger, statistics.

Runs on the Spark driver inside ``foreachBatch`` but contains no Spark imports,
so it is unit-tested directly. Exactly-once *effects* come from idempotence,
not from Spark:

- ``batch_id`` is derived from Spark's micro-batch number, which Spark keeps
  stable when a failed batch is replayed from the checkpoint.
- Everything written for a batch is deterministic in the batch's data (object
  keys, record order, and ``ingested_at`` = newest Kafka timestamp in the batch),
  so a replay produces byte-identical objects and an identical manifest.
- An existing manifest for the same batch ID with *different* content means the
  checkpoint was reset without changing ``run_id``. That is refused loudly
  (:class:`BatchConflictError`) instead of overwriting committed data.
- A batch that does not reach ``COMPLETE`` raises, so Spark does not advance the
  checkpoint and the batch is retried; committed destinations are skipped.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

from ecomm.contracts import parse_event
from ecomm.contracts.types import format_utc
from ecomm.lake.commit import BatchStatus, RetryPolicy, commit_batch, record_status
from ecomm.lake.ingest import (
    IngestedEvent,
    QuarantineRecord,
    SourceMessage,
    quarantine_bytes,
    quarantine_key,
)
from ecomm.lake.manifest import BatchManifest, manifest_key
from ecomm.lake.raw import prepare_batch
from ecomm.lake.stats import IngestStats, record_ingest_stats
from ecomm.spark.rows import STATUS_OK
from ecomm.storage.interface import ObjectNotFoundError, ObjectStore, StorageError


class BatchConflictError(RuntimeError):
    """A different batch with the same ID is already committed."""


class BatchCommitError(RuntimeError):
    """The batch did not reach COMPLETE; Spark must retry it."""


@dataclass(frozen=True)
class BatchRow:
    topic: str
    partition: int
    offset: int
    key: str | None
    value: bytes
    ts_ms: int  # Kafka message timestamp (arrival time)
    status: str
    event_id: str | None
    event_type: str | None
    event_time_iso: str | None
    is_late: bool
    error_code: str | None
    error_detail: str | None
    is_duplicate: bool


@dataclass
class SinkConfig:
    source: str
    run_id: str
    stores: Mapping[str, ObjectStore]
    control_store: ObjectStore
    required: Sequence[str] | None = None
    retry: RetryPolicy = field(default_factory=lambda: RetryPolicy(max_attempts=4))
    sleep: Callable[[float], None] = time.sleep


@dataclass(frozen=True)
class BatchOutcome:
    batch_id: str
    input_rows: int
    valid: int
    duplicates: int
    quarantined: int
    late: int
    committed: bool  # a raw batch was written (False when there were no new valid events)
    offsets: dict[str, list[int]]

    def to_json_dict(self) -> dict[str, object]:
        return {
            "batch_id": self.batch_id,
            "input_rows": self.input_rows,
            "valid": self.valid,
            "duplicates": self.duplicates,
            "quarantined": self.quarantined,
            "late": self.late,
            "committed": self.committed,
            "offsets": self.offsets,
        }


def batch_id_for(run_id: str, batch_number: int) -> str:
    return f"{run_id}-b{batch_number:05d}"


def _offsets(rows: Sequence[BatchRow]) -> dict[str, list[int]]:
    spans: dict[str, list[int]] = {}
    for r in rows:
        span = spans.setdefault(f"{r.topic}/{r.partition}", [r.offset, r.offset])
        span[0], span[1] = min(span[0], r.offset), max(span[1], r.offset)
    return dict(sorted(spans.items()))


_RESET_HINT = "If the checkpoint was reset, start the job with a new run_id."


def _check_no_conflict(
    stores: Mapping[str, ObjectStore],
    source: str,
    batch_id: str,
    manifest: BatchManifest | None,
    quarantine: bytes | None,
) -> None:
    """Refuse to overwrite anything already committed under this batch ID with different data."""
    for name, store in stores.items():
        if manifest is not None:
            try:
                existing = BatchManifest.from_bytes(
                    store.read_object(manifest_key(source, batch_id))
                )
            except (ObjectNotFoundError, ValueError):
                existing = None  # absent, or unreadable (the commit protocol rewrites it)
            if existing is not None and existing != manifest:
                raise BatchConflictError(
                    f"{name}: batch {batch_id} is already committed with different content. "
                    + _RESET_HINT
                )
        if quarantine is not None:
            try:
                old = store.read_object(quarantine_key(source, batch_id))
            except ObjectNotFoundError:
                continue
            if old != quarantine:
                raise BatchConflictError(
                    f"{name}: quarantine for batch {batch_id} already exists with different "
                    f"content. {_RESET_HINT}"
                )


def _write_quarantine(cfg: SinkConfig, batch_id: str, payload: bytes) -> None:
    """Write quarantine records to every required destination, with bounded retry."""
    key = quarantine_key(cfg.source, batch_id)
    names = list(cfg.required) if cfg.required is not None else list(cfg.stores)
    for name in names:
        error = ""
        for attempt in range(1, cfg.retry.max_attempts + 1):
            try:
                cfg.stores[name].write_object(key, payload, {"batch_id": batch_id})
            except StorageError as exc:
                error = str(exc)
                if attempt < cfg.retry.max_attempts:
                    cfg.sleep(cfg.retry.delay(attempt))
                continue
            break
        else:
            raise BatchCommitError(
                f"quarantine write to {name} failed for {batch_id}: {error}; will be retried"
            )


def process_batch(rows: Iterable[BatchRow], batch_number: int, cfg: SinkConfig) -> BatchOutcome:
    """Land one micro-batch. Safe to call again for the same ``batch_number``."""
    ordered = sorted(rows, key=lambda r: (r.topic, r.partition, r.offset))
    batch_id = batch_id_for(cfg.run_id, batch_number)
    if not ordered:
        return BatchOutcome(batch_id, 0, 0, 0, 0, 0, False, {})

    ingested_at = datetime.fromtimestamp(max(r.ts_ms for r in ordered) / 1000, UTC)
    new_events: list[IngestedEvent] = []
    quarantined: list[QuarantineRecord] = []
    duplicates = 0
    for r in ordered:
        if r.status != STATUS_OK:
            quarantined.append(
                QuarantineRecord(
                    error_code=r.error_code or "UNKNOWN",
                    error_detail=r.error_detail or "",
                    original_payload=r.value.decode("utf-8", errors="replace")[:16_384],
                    topic=r.topic,
                    partition=r.partition,
                    offset=r.offset,
                    processed_at=ingested_at,
                )
            )
        elif r.is_duplicate:
            duplicates += 1
        else:
            message = SourceMessage(
                topic=r.topic,
                partition=r.partition,
                offset=r.offset,
                key=r.key or "",
                value=r.value.decode("utf-8"),
                arrival_time=datetime.fromtimestamp(r.ts_ms / 1000, UTC),
            )
            envelope = parse_event(r.value).envelope  # already validated upstream
            new_events.append(IngestedEvent(message=message, envelope=envelope, is_late=r.is_late))

    prepared = (
        prepare_batch(
            new_events,
            source=cfg.source,
            run_id=cfg.run_id,
            batch_id=batch_id,
            ingested_at=ingested_at,
        )
        if new_events
        else None
    )
    quarantine_payload = quarantine_bytes(quarantined) if quarantined else None

    # Detect a clash with already-committed data BEFORE writing anything.
    _check_no_conflict(
        cfg.stores,
        cfg.source,
        batch_id,
        prepared.manifest if prepared else None,
        quarantine_payload,
    )
    if quarantine_payload is not None:
        _write_quarantine(cfg, batch_id, quarantine_payload)

    committed = False
    if prepared is not None:
        result = commit_batch(
            prepared, cfg.stores, required=cfg.required, retry=cfg.retry, sleep=cfg.sleep
        )
        record_status(cfg.control_store, result)
        if result.status is not BatchStatus.COMPLETE:
            raise BatchCommitError(
                f"batch {batch_id} is {result.status.value}; it will be retried from the checkpoint"
            )
        committed = True

    outcome = BatchOutcome(
        batch_id=batch_id,
        input_rows=len(ordered),
        valid=len(new_events),
        duplicates=duplicates,
        quarantined=len(quarantined),
        late=sum(1 for e in new_events if e.is_late),
        committed=committed,
        offsets=_offsets(ordered),
    )
    record_ingest_stats(
        cfg.control_store,
        cfg.source,
        IngestStats(
            run_id=batch_id,
            processed_at=ingested_at,
            input_count=outcome.input_rows,
            valid=outcome.valid,
            duplicates=outcome.duplicates,
            quarantined=outcome.quarantined,
            late=outcome.late,
        ),
    )
    return outcome


def record_progress(
    control_store: ObjectStore, source: str, outcome: BatchOutcome, duration_ms: int
) -> None:
    """Per-batch progress record (offset ranges and processing latency).

    ``duration_ms`` is wall-clock, so this file differs between a first run and a
    replay; it is observability only and nothing reads it for correctness.
    """
    payload = {
        **outcome.to_json_dict(),
        "duration_ms": duration_ms,
        "recorded_at": format_utc(datetime.now(UTC)),
    }
    key = f"spark/source={source}/batch_id={outcome.batch_id}.json"
    control_store.write_object(key, (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode())
