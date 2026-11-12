"""Shared helpers for the integration tests that run containers against the local MinIO.

Kept out of the test modules so the Airflow and exporter tests do not each carry their own copy of
the same secret-file parsing, bucket cleanup and "is the bucket clean?" guard.
"""

from __future__ import annotations

import shutil
import subprocess
import urllib.request
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config

from ecomm.storage.s3 import S3ObjectStore, client_options

REPO = Path(__file__).resolve().parents[2]
APP_ENV = REPO / "docker" / "minio" / ".env"
ROOT_ENV = REPO / "docker" / "minio" / ".env.root"
SOURCE = "commerce"


def read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip() and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                values[key.strip()] = value.strip()
    return values


def docker(*args: str, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    executable = shutil.which("docker") or "docker"
    return subprocess.run(  # noqa: S603 - fixed argv, no shell
        [executable, *args], capture_output=True, text=True, timeout=timeout, cwd=REPO, check=False
    )


def minio_ready() -> str | None:
    """None when MinIO and its secrets are usable, otherwise the reason to skip."""
    app, root = read_env(APP_ENV), read_env(ROOT_ENV)
    if (
        not {"ECOMM_S3_BUCKET", "ECOMM_S3_ACCESS_KEY", "ECOMM_S3_SECRET_KEY"} <= set(app)
        or not root
    ):
        return "MinIO secrets missing (python docker/minio/generate_env.py)"
    try:
        urllib.request.urlopen(f"{app['ECOMM_S3_ENDPOINT_URL']}/minio/health/live", timeout=5)  # noqa: S310
    except OSError:
        return "MinIO is not running (docker compose --profile minio up -d)"
    return None


def pipeline_store(app: dict[str, str]) -> S3ObjectStore:
    return S3ObjectStore(
        app["ECOMM_S3_BUCKET"],
        endpoint_url=app["ECOMM_S3_ENDPOINT_URL"],
        access_key=app["ECOMM_S3_ACCESS_KEY"],
        secret_key=app["ECOMM_S3_SECRET_KEY"],
        name="s3-minio",
    )


def root_client(app: dict[str, str], root: dict[str, str]) -> Any:
    """Admin access, used only to clean up (the pipeline user cannot delete)."""
    return boto3.client(
        "s3",
        region_name="us-east-1",
        endpoint_url=app["ECOMM_S3_ENDPOINT_URL"],
        aws_access_key_id=root["MINIO_ROOT_USER"],
        aws_secret_access_key=root["MINIO_ROOT_PASSWORD"],
        config=Config(**client_options(app["ECOMM_S3_ENDPOINT_URL"])),
    )


def foreign_commerce_batches(store: S3ObjectStore, own_marker: str) -> list[str]:
    """Committed 'commerce' manifests that are not this test's own (matched by ``own_marker``)."""
    keys = store.list_objects(f"manifests/source={SOURCE}/")
    return [k for k in keys if own_marker not in k]


def purge_marker(client: Any, bucket: str, marker: str) -> None:
    """Delete every version of every object whose key contains ``marker`` (the bucket is versioned)."""
    for prefix in ("raw/", "manifests/", "quarantine/"):
        for page in client.get_paginator("list_object_versions").paginate(
            Bucket=bucket, Prefix=prefix
        ):
            found = [
                {"Key": v["Key"], "VersionId": v["VersionId"]}
                for v in page.get("Versions", []) + page.get("DeleteMarkers", [])
                if marker in v["Key"]
            ]
            if found:
                client.delete_objects(Bucket=bucket, Delete={"Objects": found})
