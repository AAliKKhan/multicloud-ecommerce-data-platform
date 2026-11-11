"""Amazon S3 (and S3-compatible, e.g. MinIO) adapter.

Credentials come from the standard AWS chain (environment, profile, role);
nothing is read from this repository's config. Verified against ``moto`` in unit tests and
against a real S3-compatible server (MinIO) in the integration tests; **not yet verified against
real AWS** (no account available).

Timeouts are bounded on purpose: the commit layer retries storage errors with its own backoff, so
an unreachable endpoint must fail in seconds, not hang for botocore's default minute per try.
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

CONNECT_TIMEOUT_S = 5
READ_TIMEOUT_S = 30
BOTO_MAX_ATTEMPTS = 2  # the commit layer's own retry sits on top of this
DEFAULT_CUSTOM_ENDPOINT_REGION = "us-east-1"  # S3-compatible servers ignore it but boto needs one


def client_options(endpoint_url: str | None) -> dict[str, Any]:
    """Settings for the boto3 client; kept separate so they can be tested without a network."""
    options: dict[str, Any] = {
        "connect_timeout": CONNECT_TIMEOUT_S,
        "read_timeout": READ_TIMEOUT_S,
        "retries": {"max_attempts": BOTO_MAX_ATTEMPTS, "mode": "standard"},
    }
    if endpoint_url:
        # Self-hosted servers are reached by host name or IP, not as <bucket>.<host>; and they
        # do not all accept the default CRC checksum trailers newer SDKs add to every upload.
        options["s3"] = {"addressing_style": "path"}
        options["request_checksum_calculation"] = "when_required"
        options["response_checksum_validation"] = "when_required"
    return options


class S3ObjectStore:
    def __init__(
        self,
        bucket: str,
        *,
        region: str | None = None,
        endpoint_url: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        client: Any = None,
        name: str = "s3",
    ) -> None:
        if not bucket:
            raise StorageConfigError("S3 bucket name is required (AWS_S3_BUCKET)")
        if bool(access_key) != bool(secret_key):
            raise StorageConfigError("give both an access key and a secret key, or neither")
        self.name = name
        self.bucket = bucket
        if client is None:
            try:
                import boto3
            except ImportError as exc:
                raise StorageConfigError(
                    "boto3 is not installed; run: uv sync --extra aws"
                ) from exc
            from botocore.config import Config

            client = boto3.client(
                "s3",
                region_name=region or (DEFAULT_CUSTOM_ENDPOINT_REGION if endpoint_url else None),
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,  # None falls back to the standard AWS chain
                aws_secret_access_key=secret_key,
                config=Config(**client_options(endpoint_url)),
            )
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
