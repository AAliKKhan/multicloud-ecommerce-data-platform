"""Test doubles shared by unit and e2e tests (no SDKs, no I/O)."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping

from ecomm.storage.interface import ObjectNotFoundError, ObjectStore, StorageError, validate_key


class InMemoryObjectStore:
    def __init__(self, name: str = "memory") -> None:
        self.name = name
        self.objects: dict[str, bytes] = {}
        self.metadata: dict[str, dict[str, str]] = {}

    def write_object(
        self, key: str, data: bytes, metadata: Mapping[str, str] | None = None
    ) -> None:
        self.objects[validate_key(key)] = bytes(data)
        self.metadata[key] = dict(metadata or {})

    def read_object(self, key: str) -> bytes:
        try:
            return self.objects[validate_key(key)]
        except KeyError as exc:
            raise ObjectNotFoundError(key) from exc

    def list_objects(self, prefix: str = "") -> list[str]:
        return sorted(k for k in self.objects if k.startswith(prefix))

    def object_exists(self, key: str) -> bool:
        return validate_key(key) in self.objects


class FlakyObjectStore:
    """Wraps a store and fails writes on demand to exercise retry/partial paths.

    ``fail_writes`` is the number of upcoming write attempts that raise
    :class:`StorageError`; use a very large number for a permanent outage, and
    ``fail_keys_containing`` to restrict failures to matching keys.
    """

    def __init__(
        self, inner: ObjectStore, fail_writes: int = 0, fail_keys_containing: str = ""
    ) -> None:
        self.inner = inner
        self.name = inner.name
        self.fail_writes = fail_writes
        self.fail_keys_containing = fail_keys_containing
        self.write_attempts: defaultdict[str, int] = defaultdict(int)

    def write_object(
        self, key: str, data: bytes, metadata: Mapping[str, str] | None = None
    ) -> None:
        self.write_attempts[key] += 1
        if self.fail_writes > 0 and self.fail_keys_containing in key:
            self.fail_writes -= 1
            raise StorageError(f"simulated outage writing {key}")
        self.inner.write_object(key, data, metadata)

    def read_object(self, key: str) -> bytes:
        return self.inner.read_object(key)

    def list_objects(self, prefix: str = "") -> list[str]:
        return self.inner.list_objects(prefix)

    def object_exists(self, key: str) -> bool:
        return self.inner.object_exists(key)
