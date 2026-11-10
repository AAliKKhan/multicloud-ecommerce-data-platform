"""Minimal fake of the google-cloud-storage client surface used by GCSObjectStore."""

from __future__ import annotations

from typing import Any


class NotFound(Exception):  # noqa: N818 - name mirrors google.api_core.exceptions.NotFound
    pass


class FakeBlob:
    def __init__(self, bucket: FakeBucket, name: str) -> None:
        self._bucket = bucket
        self.name = name
        self.metadata: dict[str, str] | None = None

    def upload_from_string(self, data: bytes, content_type: str = "") -> None:
        self._bucket.data[self.name] = (bytes(data), dict(self.metadata or {}))

    def download_as_bytes(self) -> bytes:
        if self.name not in self._bucket.data:
            raise NotFound(self.name)
        return self._bucket.data[self.name][0]

    def exists(self) -> bool:
        return self.name in self._bucket.data


class FakeBucket:
    def __init__(self, name: str) -> None:
        self.name = name
        self.data: dict[str, tuple[bytes, dict[str, str]]] = {}

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(self, name)


class FakeGCSClient:
    def __init__(self) -> None:
        self.buckets: dict[str, FakeBucket] = {}

    def bucket(self, name: str) -> FakeBucket:
        return self.buckets.setdefault(name, FakeBucket(name))

    def list_blobs(self, bucket: str, prefix: str = "") -> list[Any]:
        data = self.bucket(bucket).data
        return [FakeBlob(self.bucket(bucket), n) for n in data if n.startswith(prefix)]
