"""Reference (pure-Python) ingest logic: validate, de-duplicate, quarantine.

This is the executable specification of what the Spark job must do (M3) and
is used by tests as an oracle. It is also what the Docker-free demo uses.

Policy:
- Valid + first sighting of an ``event_id``   -> ``valid``
- Valid + ``event_id`` already seen            -> ``duplicates`` (counted, not written)
- Fails ``parse_event``                         -> ``quarantined`` with code and payload
- Late events (arrival much later than event time) are *kept* and flagged;
  they are never dropped (see ADR 0002).
"""

from __future__ import annotations

import json
from collections.abc import Iterable, MutableSet
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from ecomm.contracts import ContractViolation, parse_event
from ecomm.contracts.envelope import Envelope
from ecomm.contracts.types import format_utc
from ecomm.storage.interface import ObjectStore

MAX_QUARANTINED_PAYLOAD_CHARS = 16_384


@dataclass(frozen=True)
class SourceMessage:
    """One Kafka message as read by the consumer."""

    topic: str
    partition: int
    offset: int
    key: str
    value: str
    arrival_time: datetime


@dataclass(frozen=True)
class IngestedEvent:
    message: SourceMessage
    envelope: Envelope
    is_late: bool


@dataclass(frozen=True)
class QuarantineRecord:
    error_code: str
    error_detail: str
    original_payload: str
    topic: str
    partition: int
    offset: int
    processed_at: datetime

    def to_json_dict(self) -> dict[str, object]:
        return {
            "error_code": self.error_code,
            "error_detail": self.error_detail,
            "original_payload": self.original_payload,
            "topic": self.topic,
            "partition": self.partition,
            "offset": self.offset,
            "processed_at": format_utc(self.processed_at),
        }


@dataclass
class ClassifiedBatch:
    valid: list[IngestedEvent] = field(default_factory=list)
    duplicates: list[IngestedEvent] = field(default_factory=list)
    quarantined: list[QuarantineRecord] = field(default_factory=list)

    @property
    def late_count(self) -> int:
        return sum(1 for e in self.valid if e.is_late)

    @property
    def input_count(self) -> int:
        return len(self.valid) + len(self.duplicates) + len(self.quarantined)


def classify(
    messages: Iterable[SourceMessage],
    seen_event_ids: MutableSet[str],
    *,
    processed_at: datetime,
    late_threshold: timedelta,
) -> ClassifiedBatch:
    """Classify messages in order. ``seen_event_ids`` is updated in place so
    callers can carry de-duplication state from one micro-batch to the next."""
    result = ClassifiedBatch()
    for message in messages:
        try:
            parsed = parse_event(message.value)
        except ContractViolation as violation:
            result.quarantined.append(
                QuarantineRecord(
                    error_code=violation.code.value,
                    error_detail=violation.detail,
                    original_payload=message.value[:MAX_QUARANTINED_PAYLOAD_CHARS],
                    topic=message.topic,
                    partition=message.partition,
                    offset=message.offset,
                    processed_at=processed_at,
                )
            )
            continue
        envelope = parsed.envelope
        late = message.arrival_time - envelope.event_time > late_threshold
        event = IngestedEvent(message=message, envelope=envelope, is_late=late)
        event_id = str(envelope.event_id)
        if event_id in seen_event_ids:
            result.duplicates.append(event)
        else:
            seen_event_ids.add(event_id)
            result.valid.append(event)
    return result


def quarantine_key(source: str, batch_id: str) -> str:
    return f"quarantine/source={source}/batch_id={batch_id}/part-0.jsonl"


def write_quarantine(
    store: ObjectStore, source: str, batch_id: str, records: list[QuarantineRecord]
) -> str | None:
    """Persist quarantined messages (deterministic key, safe to retry)."""
    if not records:
        return None
    lines = (json.dumps(r.to_json_dict(), sort_keys=True, separators=(",", ":")) for r in records)
    key = quarantine_key(source, batch_id)
    store.write_object(key, ("\n".join(lines) + "\n").encode(), {"batch_id": batch_id})
    return key
