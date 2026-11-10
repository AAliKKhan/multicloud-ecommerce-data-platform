"""Google Cloud Storage adapter.

Authentication uses Application Default Credentials (``gcloud auth
application-default login``, a workload identity, or a service-account file
referenced by ``GOOGLE_APPLICATION_CREDENTIALS`` *outside* this repository).
Credentials are checked up front so a misconfiguration fails with an
actionable message. Unit-tested with a fake client only: **not yet verified
against real Google Cloud** (no account available, and GCS has no emulator in
this project).
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


class GCSObjectStore:
    def __init__(
        self,
        bucket: str,
        *,
        project: str | None = None,
        client: Any = None,
        name: str = "gcs",
    ) -> None:
        if not bucket:
            raise StorageConfigError("GCS bucket name is required (GCS_BUCKET)")
        self.name = name
        self.bucket_name = bucket
        if client is None:
            client = self._default_client(project)
        self._client = client
        self._bucket = client.bucket(bucket)

    @staticmethod
    def _default_client(project: str | None) -> Any:
        try:
            import google.auth
            from google.auth.exceptions import DefaultCredentialsError
            from google.cloud import storage
        except ImportError as exc:
            raise StorageConfigError(
                "google-cloud-storage is not installed; run: uv sync --extra gcp"
            ) from exc
        try:
            credentials, default_project = google.auth.default()
        except DefaultCredentialsError as exc:
            raise StorageConfigError(
                "No Google credentials found. Run 'gcloud auth application-default login' "
                "or set GOOGLE_APPLICATION_CREDENTIALS to a key file stored outside the repo."
            ) from exc
        return storage.Client(project=project or default_project, credentials=credentials)

    @staticmethod
    def _is_not_found(exc: Exception) -> bool:
        return type(exc).__name__ == "NotFound"

    def write_object(
        self, key: str, data: bytes, metadata: Mapping[str, str] | None = None
    ) -> None:
        validate_key(key)
        try:
            blob = self._bucket.blob(key)
            blob.metadata = dict(metadata or {})
            blob.upload_from_string(data, content_type="application/octet-stream")
        except Exception as exc:
            raise StorageError(f"GCS write failed for {key}: {type(exc).__name__}") from exc

    def read_object(self, key: str) -> bytes:
        validate_key(key)
        try:
            data: bytes = self._bucket.blob(key).download_as_bytes()
            return data
        except Exception as exc:
            if self._is_not_found(exc):
                raise ObjectNotFoundError(key) from exc
            raise StorageError(f"GCS read failed for {key}: {type(exc).__name__}") from exc

    def list_objects(self, prefix: str = "") -> list[str]:
        try:
            blobs = self._client.list_blobs(self.bucket_name, prefix=prefix)
            return sorted(blob.name for blob in blobs)
        except Exception as exc:
            raise StorageError(f"GCS list failed for {prefix!r}: {type(exc).__name__}") from exc

    def object_exists(self, key: str) -> bool:
        validate_key(key)
        try:
            exists: bool = self._bucket.blob(key).exists()
            return exists
        except Exception as exc:
            raise StorageError(f"GCS exists failed for {key}: {type(exc).__name__}") from exc
