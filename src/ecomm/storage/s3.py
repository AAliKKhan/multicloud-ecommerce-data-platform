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
# What S3 answers for a missing key when the caller has no unconditioned s3:ListBucket (AWS), and
# also for a key that exists but may not be read. The adapter must tell the two apart.
_DENIED_CODES = {"AccessDenied", "403", "Forbidden"}

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

    def _code(self, exc: Exception) -> str:
        return str(getattr(exc, "response", {}).get("Error", {}).get("Code"))

    def _is_not_found(self, exc: Exception) -> bool:
        return self._code(exc) in _NOT_FOUND_CODES

    def _is_denied(self, exc: Exception) -> bool:
        return self._code(exc) in _DENIED_CODES

    def _absent_when_denied(self, key: str, operation: str) -> bool:
        """Is a key that answered 403 really absent? True: absent. False: it exists.

        AWS answers 403 instead of 404 for a missing key unless the caller has an unconditioned
        ``s3:ListBucket``. The Terraform pipeline role grants ListBucket only under an
        ``s3:prefix`` condition, which a plain GET or HEAD does not carry. A prefix list of exactly
        this key does carry it, and is allowed anywhere the role may write, so it settles the
        question without widening the role. If even the list is refused we cannot tell, and that
        is reported as a permission error, never guessed as "missing" (a wrong guess would let a
        batch overwrite committed data).
        """
        try:
            found = self._client.list_objects_v2(Bucket=self.bucket, Prefix=key, MaxKeys=1)
        except Exception as exc:
            raise StorageError(
                f"S3 permission denied for {key} ({operation}) and the prefix probe was refused "
                f"too ({type(exc).__name__}): cannot tell a missing object from a forbidden one"
            ) from exc
        # A key sorts before every longer key sharing its prefix, so if it exists it is first.
        return not any(item.get("Key") == key for item in found.get("Contents", []))

    def _denied(self, key: str, operation: str, exc: Exception) -> StorageError:
        return StorageError(
            f"S3 permission denied for {key} ({operation}): the object exists but this role "
            f"may not {operation} it ({type(exc).__name__})"
        )

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
            if self._is_denied(exc):
                if self._absent_when_denied(key, "read"):
                    raise ObjectNotFoundError(key) from exc
                raise self._denied(key, "read", exc) from exc
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
            if self._is_denied(exc):
                if self._absent_when_denied(key, "check"):
                    return False
                raise self._denied(key, "check", exc) from exc
            raise StorageError(f"S3 head failed for {key}: {type(exc).__name__}") from exc
        return True
