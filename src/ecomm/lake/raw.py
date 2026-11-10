"""Build a raw-zone batch: deterministic partitioned JSONL objects + manifest.

Object key layout (ADR 0003 / 0006)::

    raw/<source>/<event_type>/event_date=YYYY-MM-DD/event_hour=HH/part-<batch_id>.jsonl

Determinism is what makes retries safe: the same logical batch always yields
byte-identical objects at identical keys, so a retried write overwrites with
the same bytes and checksums agree across clouds.

Each line is ``{"ingest": {...lineage...}, "event": <original envelope>}``.
The original event is never modified (raw is immutable); lineage is added
next to it.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

from ecomm.contracts.types import format_utc
from ecomm.lake.ingest import IngestedEvent
from ecomm.lake.manifest import (
    BatchManifest,
    ManifestObject,
    logical_checksum,
    sha256_hex,
)

RAW_PREFIX = "raw/"


def raw_key(source: str, event_type: str, event_time: datetime, batch_id: str) -> str:
    return (
        f"raw/{source}/{event_type}/event_date={event_time:%Y-%m-%d}"
        f"/event_hour={event_time:%H}/part-{batch_id}.jsonl"
    )


@dataclass(frozen=True)
class PreparedBatch:
    manifest: BatchManifest
    objects: dict[str, bytes]  # key -> bytes


def _line(event: IngestedEvent, run_id: str, batch_id: str, ingested_at: datetime) -> str:
    message = event.message
    record = {
        "ingest": {
            "run_id": run_id,
            "batch_id": batch_id,
            "source_topic": message.topic,
            "kafka_partition": message.partition,
            "kafka_offset": message.offset,
            "ingested_at": format_utc(ingested_at),
            "is_late": event.is_late,
        },
        "event": json.loads(message.value),
    }
    return json.dumps(record, sort_keys=True, separators=(",", ":"))


def prepare_batch(
    events: list[IngestedEvent],
    *,
    source: str,
    run_id: str,
    batch_id: str,
    ingested_at: datetime,
) -> PreparedBatch:
    """Group events into partitioned objects and describe them in a manifest."""
    if not events:
        raise ValueError("cannot prepare an empty batch")
    ids = [str(e.envelope.event_id) for e in events]
    if len(set(ids)) != len(ids):
        raise ValueError("batch contains duplicate event_ids; de-duplicate before landing")

    groups: dict[str, list[IngestedEvent]] = defaultdict(list)
    for event in events:
        envelope = event.envelope
        key = raw_key(source, envelope.event_type.value, envelope.event_time, batch_id)
        groups[key].append(event)

    objects: dict[str, bytes] = {}
    manifest_objects: list[ManifestObject] = []
    for key in sorted(groups):
        ordered = sorted(
            groups[key], key=lambda e: (e.envelope.event_time, str(e.envelope.event_id))
        )
        lines = [_line(e, run_id, batch_id, ingested_at) for e in ordered]
        data = ("\n".join(lines) + "\n").encode()
        objects[key] = data
        manifest_objects.append(
            ManifestObject(
                key=key, record_count=len(lines), size_bytes=len(data), sha256=sha256_hex(data)
            )
        )

    offsets: dict[str, list[int]] = {}
    for event in events:
        m = event.message
        span = offsets.setdefault(f"{m.topic}/{m.partition}", [m.offset, m.offset])
        span[0], span[1] = min(span[0], m.offset), max(span[1], m.offset)

    times = [e.envelope.event_time for e in events]
    manifest = BatchManifest(
        batch_id=batch_id,
        run_id=run_id,
        source=source,
        ingested_at=ingested_at,
        event_count=len(events),
        min_event_time=min(times),
        max_event_time=max(times),
        logical_checksum=logical_checksum(ids),
        source_offsets=dict(sorted(offsets.items())),
        objects=manifest_objects,
    )
    return PreparedBatch(manifest=manifest, objects=objects)


def event_ids_in_object(data: bytes) -> list[str]:
    """Extract event IDs from a raw object (used by parity checks and loaders)."""
    ids = []
    for line in data.decode().splitlines():
        if line.strip():
            ids.append(str(json.loads(line)["event"]["event_id"]))
    return ids
