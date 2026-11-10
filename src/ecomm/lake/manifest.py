"""Batch manifests: the commit record and parity fingerprint of a raw batch.

Two kinds of checksum are kept deliberately:

- ``ManifestObject.sha256`` fingerprints the *bytes* of one stored object.
- ``logical_checksum`` fingerprints the *set of event IDs* in the batch, so two
  clouds can be compared record-by-record regardless of file layout.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable

from pydantic import BaseModel, ConfigDict, Field

from ecomm.contracts.types import UtcDateTime

MANIFEST_VERSION = 1


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def logical_checksum(event_ids: Iterable[str]) -> str:
    """Order-independent fingerprint of a set of event IDs."""
    return sha256_hex("\n".join(sorted(event_ids)).encode())


def manifest_key(source: str, batch_id: str) -> str:
    return f"manifests/source={source}/batch_id={batch_id}/manifest.json"


def manifest_prefix(source: str) -> str:
    return f"manifests/source={source}/"


class ManifestObject(BaseModel):
    model_config = ConfigDict(frozen=True)

    key: str
    record_count: int = Field(ge=0)
    size_bytes: int = Field(ge=0)
    sha256: str


class BatchManifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    manifest_version: int = MANIFEST_VERSION
    batch_id: str
    run_id: str
    source: str
    ingested_at: UtcDateTime
    event_count: int = Field(ge=0)
    min_event_time: UtcDateTime | None
    max_event_time: UtcDateTime | None
    logical_checksum: str
    # "topic/partition" -> [min_offset, max_offset]
    source_offsets: dict[str, list[int]] = Field(default_factory=dict)
    objects: list[ManifestObject]

    def to_bytes(self) -> bytes:
        return (self.model_dump_json(indent=2) + "\n").encode()

    @classmethod
    def from_bytes(cls, data: bytes) -> BatchManifest:
        return cls.model_validate_json(data)
