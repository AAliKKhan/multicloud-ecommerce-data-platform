"""Per-message classification used by the Spark job (no Spark imports).

Kept as plain Python so the exact same logic runs inside the Spark UDF and in
unit tests. It reuses :func:`ecomm.contracts.parse_event`, the single validator
shared with the producer boundary and the pure-Python reference ingest.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from ecomm.contracts import ContractViolation, parse_event
from ecomm.contracts.types import format_utc

STATUS_OK = "OK"
STATUS_INVALID = "INVALID"


@dataclass(frozen=True)
class RowVerdict:
    status: str  # STATUS_OK | STATUS_INVALID
    event_id: str | None
    event_type: str | None
    event_time_iso: str | None
    is_late: bool
    error_code: str | None
    error_detail: str | None


def classify_value(value: bytes | None, arrival: datetime, late_threshold: timedelta) -> RowVerdict:
    """Validate one Kafka message value.

    ``arrival`` is the Kafka message timestamp. A message is *late* when it
    arrived more than ``late_threshold`` after its own event time. Late
    messages are kept and flagged, never dropped (ADR 0002).
    """
    try:
        parsed = parse_event(value if value is not None else b"")
    except ContractViolation as violation:
        return RowVerdict(
            STATUS_INVALID, None, None, None, False, violation.code.value, violation.detail
        )
    envelope = parsed.envelope
    return RowVerdict(
        status=STATUS_OK,
        event_id=str(envelope.event_id),
        event_type=envelope.event_type.value,
        event_time_iso=format_utc(envelope.event_time),
        is_late=arrival - envelope.event_time > late_threshold,
        error_code=None,
        error_detail=None,
    )


def dedup_key(
    verdict_status: str, event_id: str | None, topic: str, partition: int, offset: int
) -> str:
    """State key: the ``event_id`` for valid events; a unique per-message key for invalid
    ones so they flow through the stateful step without ever being de-duplicated."""
    if verdict_status == STATUS_OK and event_id:
        return event_id
    return f"invalid:{topic}:{partition}:{offset}"
