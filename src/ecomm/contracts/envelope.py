"""Event envelope plus a single validation entry point used at both the
producer and the consumer boundary.

``parse_event`` never returns partially-valid data: it either returns a fully
validated event or raises :class:`ContractViolation` with a stable, low
cardinality ``code`` (suitable for quarantine records and metric labels) and a
detail string that contains field names and messages but never input values.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from ecomm.contracts.events import (
    PAYLOAD_MODELS,
    SUPPORTED_SCHEMA_VERSIONS,
    EventType,
)
from ecomm.contracts.payloads import Payload
from ecomm.contracts.types import UtcDateTime

REQUIRED_ENVELOPE_FIELDS = (
    "event_id",
    "event_type",
    "schema_version",
    "event_time",
    "producer",
    "correlation_id",
    "payload",
)


class Envelope(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    event_id: UUID
    event_type: EventType
    schema_version: int
    event_time: UtcDateTime
    producer: str = Field(min_length=1, max_length=100)
    correlation_id: UUID
    payload: dict[str, Any]

    def to_json(self) -> str:
        return self.model_dump_json()


class ViolationCode(StrEnum):
    INVALID_JSON = "INVALID_JSON"
    NOT_AN_OBJECT = "NOT_AN_OBJECT"
    MISSING_FIELD = "MISSING_FIELD"
    INVALID_FIELD = "INVALID_FIELD"
    UNKNOWN_EVENT_TYPE = "UNKNOWN_EVENT_TYPE"
    UNSUPPORTED_SCHEMA_VERSION = "UNSUPPORTED_SCHEMA_VERSION"
    INVALID_PAYLOAD = "INVALID_PAYLOAD"


class ContractViolation(ValueError):  # noqa: N818 - domain term, not an error suffix
    def __init__(self, code: ViolationCode, detail: str) -> None:
        super().__init__(f"{code.value}: {detail}")
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class ParsedEvent:
    envelope: Envelope
    payload: Payload


def _summarise(error: ValidationError) -> str:
    parts = []
    for err in error.errors(include_input=False, include_url=False, include_context=False):
        location = ".".join(str(part) for part in err["loc"]) or "<root>"
        parts.append(f"{location}: {err['msg']}")
    return "; ".join(parts)


def parse_event(raw: str | bytes) -> ParsedEvent:
    """Validate a raw Kafka message value. Raises :class:`ContractViolation`."""
    try:
        data = json.loads(raw)
    except ValueError as exc:  # JSONDecodeError and UnicodeDecodeError
        raise ContractViolation(ViolationCode.INVALID_JSON, type(exc).__name__) from exc
    if not isinstance(data, dict):
        raise ContractViolation(ViolationCode.NOT_AN_OBJECT, type(data).__name__)

    missing = [name for name in REQUIRED_ENVELOPE_FIELDS if name not in data]
    if missing:
        raise ContractViolation(ViolationCode.MISSING_FIELD, ", ".join(missing))

    if data["event_type"] not in {t.value for t in EventType}:
        raise ContractViolation(ViolationCode.UNKNOWN_EVENT_TYPE, "event_type is not recognised")

    version = data["schema_version"]
    if isinstance(version, bool) or not isinstance(version, int):
        raise ContractViolation(ViolationCode.INVALID_FIELD, "schema_version: must be an integer")
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        raise ContractViolation(
            ViolationCode.UNSUPPORTED_SCHEMA_VERSION, f"schema_version {version} is not supported"
        )

    try:
        envelope = Envelope.model_validate(data)
    except ValidationError as exc:
        raise ContractViolation(ViolationCode.INVALID_FIELD, _summarise(exc)) from exc

    try:
        payload = PAYLOAD_MODELS[envelope.event_type].model_validate(envelope.payload)
    except ValidationError as exc:
        raise ContractViolation(ViolationCode.INVALID_PAYLOAD, _summarise(exc)) from exc
    return ParsedEvent(envelope=envelope, payload=payload)
