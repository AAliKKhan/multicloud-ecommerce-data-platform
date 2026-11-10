"""Fault injection: turns clean events into a realistic, messy *arrival* stream.

Each :class:`Emission` is what a Kafka producer would send. The ``fault``
label is ground truth for tests and demos; it is never part of the message.

- ``MALFORMED``    value is corrupted so that ``parse_event`` rejects it.
- ``LATE``         arrives 1.5x-4x ``late_threshold_minutes`` after event time.
- ``OUT_OF_ORDER`` arrives up to two minutes after its event time.
- ``DUPLICATE``    an extra copy (same ``event_id``) arriving shortly after.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any

from ecomm.contracts.envelope import Envelope
from ecomm.contracts.events import message_key, topic_name
from ecomm.generator.config import GeneratorConfig


class FaultKind(StrEnum):
    MALFORMED = "malformed"
    LATE = "late"
    OUT_OF_ORDER = "out_of_order"
    DUPLICATE = "duplicate"


@dataclass(frozen=True)
class Emission:
    seq: int
    arrival_time: datetime
    topic: str
    key: str
    value: str  # raw message value; may be invalid JSON when malformed
    event_id: str  # id of the originating event (even if the value lost it)
    event_type: str
    event_time: datetime
    fault: FaultKind | None = None
    fault_detail: str | None = None


def _corrupt(envelope: Envelope, rng: random.Random) -> tuple[str, str]:
    """Return ``(raw_value, corruption_name)``; the value always fails validation."""
    text = envelope.to_json()
    data: dict[str, Any] = json.loads(text)
    payload: dict[str, Any] = data["payload"]

    def dumped(obj: dict[str, Any]) -> str:
        return json.dumps(obj, separators=(",", ":"))

    options: list[str] = [
        "truncated_json",
        "not_json_text",
        "missing_event_id",
        "unknown_event_type",
        "bad_event_time",
        "bad_schema_version",
        "missing_payload_field",
        "empty_payload",
    ]
    for amount_field in ("amount", "total_amount"):
        if amount_field in payload:
            options.append("negative_amount")
            break
    name = rng.choice(options)

    if name == "truncated_json":
        return text[: rng.randrange(10, len(text) - 1)], name
    if name == "not_json_text":
        return f"BOGUS-{rng.getrandbits(32):08x} <html>not an event</html>", name
    if name == "missing_event_id":
        del data["event_id"]
    elif name == "unknown_event_type":
        data["event_type"] = "order_exploded"
    elif name == "bad_event_time":
        data["event_time"] = "not-a-timestamp"
    elif name == "bad_schema_version":
        data["schema_version"] = "one"
    elif name == "missing_payload_field":
        del payload[next(iter(payload))]  # first field is always a required ID
    elif name == "empty_payload":
        data["payload"] = {}
    else:  # negative_amount
        field = "amount" if "amount" in payload else "total_amount"
        payload[field] = "-1.00"
    return dumped(data), name


def inject_faults(events: list[Envelope], config: GeneratorConfig) -> list[Emission]:
    """Return emissions ordered by arrival time (stable, deterministic)."""
    rng = random.Random(f"{config.seed}:faults")
    staged: list[tuple[datetime, int, Emission]] = []

    def stage(emission: Emission) -> None:
        staged.append((emission.arrival_time, len(staged), emission))

    malformed_cut = config.malformed_rate
    late_cut = malformed_cut + config.late_rate
    ooo_cut = late_cut + config.out_of_order_rate

    for envelope in events:
        payload = envelope.payload
        base: dict[str, Any] = {
            "seq": 0,
            "topic": topic_name(envelope.event_type),
            "key": message_key(envelope.event_type, payload),
            "event_id": str(envelope.event_id),
            "event_type": envelope.event_type.value,
            "event_time": envelope.event_time,
        }
        draw = rng.random()
        if draw < malformed_cut:
            value, detail = _corrupt(envelope, rng)
            stage(
                Emission(
                    **base,
                    arrival_time=envelope.event_time,
                    value=value,
                    fault=FaultKind.MALFORMED,
                    fault_detail=detail,
                )
            )
            continue

        fault: FaultKind | None = None
        arrival = envelope.event_time
        if draw < late_cut:
            fault = FaultKind.LATE
            minutes = config.late_threshold_minutes * rng.uniform(1.5, 4.0)
            arrival += timedelta(milliseconds=int(minutes * 60_000))
        elif draw < ooo_cut:
            fault = FaultKind.OUT_OF_ORDER
            arrival += timedelta(milliseconds=int(rng.uniform(1, 120) * 1000))

        value = envelope.to_json()
        stage(Emission(**base, arrival_time=arrival, value=value, fault=fault))

        if rng.random() < config.duplicate_rate:
            copy_arrival = arrival + timedelta(milliseconds=int(rng.uniform(0.1, 300) * 1000))
            stage(
                Emission(
                    **base,
                    arrival_time=copy_arrival,
                    value=value,
                    fault=FaultKind.DUPLICATE,
                )
            )

    staged.sort(key=lambda item: (item[0], item[1]))
    return [
        Emission(**{**emission.__dict__, "seq": seq}) for seq, (_, _, emission) in enumerate(staged)
    ]
