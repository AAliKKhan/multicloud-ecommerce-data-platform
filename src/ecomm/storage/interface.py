"""Provider-neutral object storage interface.

Spark and pipeline code depend only on this protocol; SDK-specific code lives
in the adapters (:mod:`ecomm.storage.local`, ``s3``, ``gcs``).

Contract (verified for every adapter by ``tests/unit/storage/test_contract.py``):

- ``write_object`` overwrites an existing key (retries are safe) and stores
  string metadata alongside the object.
- ``read_object`` raises :class:`ObjectNotFoundError` for a missing key.
- ``list_objects`` returns keys under a prefix in sorted order.
- Keys use ``/`` separators, never start with ``/`` and contain no ``..``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol, runtime_checkable


class StorageError(Exception):
    """A storage operation failed (possibly transiently)."""


class ObjectNotFoundError(StorageError):
    """The requested key does not exist."""


class StorageConfigError(StorageError):
    """The adapter is not configured (missing SDK, bucket or credentials)."""


@runtime_checkable
class ObjectStore(Protocol):
    name: str

    def write_object(
        self, key: str, data: bytes, metadata: Mapping[str, str] | None = None
    ) -> None: ...

    def read_object(self, key: str) -> bytes: ...

    def list_objects(self, prefix: str = "") -> list[str]: ...

    def object_exists(self, key: str) -> bool: ...


def validate_key(key: str) -> str:
    """Reject keys that could escape a root or behave differently per provider."""
    if not key or key.startswith("/") or "\\" in key or "\x00" in key:
        raise ValueError(f"invalid object key: {key!r}")
    if any(part in ("", ".", "..") for part in key.split("/")):
        raise ValueError(f"invalid object key: {key!r}")
    return key
