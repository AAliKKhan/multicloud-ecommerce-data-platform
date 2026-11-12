"""A boto3-shaped S3 client that behaves like AWS under the Terraform pipeline role.

AWS answers ``AccessDenied`` (403), not ``NoSuchKey`` (404), for a missing key when the caller lacks
an unconditioned ``s3:ListBucket`` on the bucket. The Terraform pipeline role grants ``ListBucket``
only under an ``s3:prefix`` condition, and a plain GET/HEAD carries no prefix, so the condition does
not match and the answer is 403. ``MinIO`` answers 404 in the same situation, which is why this
double exists: it is the worst case the real server cannot show.

The double implements only what the adapter calls. Permissions mirror the Terraform role:

* Get/Put objects under the allowed prefixes only;
* List only when the requested ``Prefix`` matches the allowed prefixes (a ``StringLike`` test);
* a missing key behaves per ``missing_key_status`` ("403" is the worst case).
"""

from __future__ import annotations

import fnmatch
from typing import Any

PIPELINE_PREFIXES = ("raw/", "manifests/", "quarantine/", "ledger/", "reports/")


class ClientError(Exception):
    """Shaped like botocore's ClientError (the adapter only reads ``.response``)."""

    def __init__(self, code: str, operation: str) -> None:
        super().__init__(f"An error occurred ({code}) when calling the {operation} operation")
        self.response = {"Error": {"Code": code, "Message": code}}


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class AwsLikeS3Client:
    def __init__(
        self,
        *,
        missing_key_status: str = "403",
        allowed_prefixes: tuple[str, ...] = PIPELINE_PREFIXES,
        list_allowed: bool = True,
        deny_get_for: tuple[str, ...] = (),
    ) -> None:
        self.objects: dict[str, bytes] = {}
        self.missing_key_status = missing_key_status
        self.allowed_prefixes = allowed_prefixes
        self.list_allowed = list_allowed  # False: no ListBucket at all (not even prefixed)
        self.deny_get_for = deny_get_for  # existing keys the caller may not read
        self.calls: list[tuple[str, str]] = []

    # --- permission model -------------------------------------------------------------------------

    def _object_allowed(self, key: str) -> bool:
        return any(fnmatch.fnmatch(key, f"{p}*") for p in self.allowed_prefixes)

    def _list_allowed_for(self, prefix: str) -> bool:
        if not self.list_allowed:
            return False
        patterns = [*self.allowed_prefixes, *(f"{p}*" for p in self.allowed_prefixes)]
        return any(fnmatch.fnmatch(prefix, pattern) for pattern in patterns)

    def _missing_code(self, operation: str) -> str:
        if self.missing_key_status == "404":
            return "NoSuchKey" if operation == "GetObject" else "404"
        return "AccessDenied" if operation == "GetObject" else "403"

    # --- the calls the adapter makes ----------------------------------------------------------------

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, **_: Any) -> dict[str, Any]:  # noqa: N803
        self.calls.append(("put", Key))
        if not self._object_allowed(Key):
            raise ClientError("AccessDenied", "PutObject")
        self.objects[Key] = bytes(Body)
        return {}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        self.calls.append(("get", Key))
        if not self._object_allowed(Key) or Key in self.deny_get_for:
            raise ClientError("AccessDenied", "GetObject")
        if Key not in self.objects:
            raise ClientError(self._missing_code("GetObject"), "GetObject")
        return {"Body": _Body(self.objects[Key])}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        self.calls.append(("head", Key))
        if not self._object_allowed(Key) or Key in self.deny_get_for:
            raise ClientError("403", "HeadObject")
        if Key not in self.objects:
            raise ClientError(self._missing_code("HeadObject"), "HeadObject")
        return {}

    def list_objects_v2(self, *, Bucket: str, Prefix: str = "", **_: Any) -> dict[str, Any]:  # noqa: N803
        self.calls.append(("list", Prefix))
        if not self._list_allowed_for(Prefix):
            raise ClientError("AccessDenied", "ListObjectsV2")
        return {
            "Contents": [{"Key": k} for k in sorted(self.objects) if k.startswith(Prefix)],
            "IsTruncated": False,
        }

    def get_paginator(self, name: str) -> Any:
        assert name == "list_objects_v2"
        client = self

        class _Paginator:
            def paginate(self, *, Bucket: str, Prefix: str = "") -> list[dict[str, Any]]:  # noqa: N803
                return [client.list_objects_v2(Bucket=Bucket, Prefix=Prefix)]

        return _Paginator()
