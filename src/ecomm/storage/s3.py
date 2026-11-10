"""Amazon S3 (and S3-compatible, e.g. MinIO) adapter.

Credentials come from the standard AWS chain (environment, profile, role);
nothing is read from this repository's config. Verified against ``moto`` in
unit tests; **not yet verified against real AWS** (no account available).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ecomm.storage.interface import (
    ObjectNotFoundError,
    StorageConfigError,
    StorageError,
    validate_key,
)

_NOT_FOUND_CODES = {"NoSuchKey", "404", "NotFound"}


class S3ObjectStore:
    def __init__(
        self,
        bucket: str,
        *,
        region: str | None = None,
        endpoint_url: str | None = None,
        client: Any = None,
        name: str = "s3",
    ) -> None:
        if not bucket:
            raise StorageConfigError("S3 bucket name is required (AWS_S3_BUCKET)")
        self.name = name
        self.bucket = bucket
        if client is None:
            try:
                import boto3
            except ImportError as exc:
                raise StorageConfigError(
                    "boto3 is not installed; run: uv sync --extra aws"
                ) from exc
            client = boto3.client("s3", region_name=region, endpoint_url=endpoint_url)
        self._client = client

    def _is_not_found(self, exc: Exception) -> bool:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        return str(code) in _NOT_FOUND_CODES

    def write_object(
        self, key: str, data: bytes, metadata: Mapping[str, str] | None = None
    ) -> None:
        validate_key(key)
        try:
            self._client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=data,
                Metadata=dict(metadata or {}),
            )
        except Exception as exc:
            raise StorageError(f"S3 write failed for {key}: {type(exc).__name__}") from exc

    def read_object(self, key: str) -> bytes:
        validate_key(key)
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=key)
            body: bytes = response["Body"].read()
            return body
        except Exception as exc:
            if self._is_not_found(exc):
                raise ObjectNotFoundError(key) from exc
            raise StorageError(f"S3 read failed for {key}: {type(exc).__name__}") from exc

    def list_objects(self, prefix: str = "") -> list[str]:
        keys: list[str] = []
        try:
            paginator = self._client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                keys.extend(item["Key"] for item in page.get("Contents", []))
        except Exception as exc:
            raise StorageError(f"S3 list failed for {prefix!r}: {type(exc).__name__}") from exc
        return sorted(keys)

    def object_exists(self, key: str) -> bool:
        validate_key(key)
        try:
            self._client.head_object(Bucket=self.bucket, Key=key)
        except Exception as exc:
            if self._is_not_found(exc):
                return False
            raise StorageError(f"S3 head failed for {key}: {type(exc).__name__}") from exc
        return True
