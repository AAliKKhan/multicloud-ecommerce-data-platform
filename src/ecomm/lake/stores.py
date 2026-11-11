"""Where the raw zone lives: one place that builds the lake's object stores.

Every command that touches the lake (demo, Spark job, warehouse load, reconciliation, quality,
metrics, quarantine) asks :func:`open_lake` for its stores, so the choice is made once:

* ``ECOMM_S3_BACKEND=local`` (default): the S3 side is a local directory, ``<root>/lake/s3-local``.
* ``ECOMM_S3_BACKEND=minio``: the S3 side is a bucket on a MinIO server, reached through the same
  :class:`~ecomm.storage.s3.S3ObjectStore` adapter that will talk to real AWS S3 (only the endpoint
  differs). The destination is then named ``s3-minio`` so its origin is never mistaken for AWS.

The GCS side (``gcs-local``) and the control store (ledger, statistics, reports) stay local
directories in both modes. MinIO settings come from the process environment, or from the
git-ignored ``docker/minio/.env`` (the process environment wins, so CI can inject its own):

    ECOMM_S3_ENDPOINT_URL   default http://127.0.0.1:9000 (use http://minio:9000 inside compose)
    ECOMM_S3_BUCKET         bucket created by the init job
    ECOMM_S3_ACCESS_KEY     the least-privilege pipeline user (never the MinIO root user)
    ECOMM_S3_SECRET_KEY
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from ecomm.storage.interface import ObjectStore, StorageConfigError
from ecomm.storage.local import LocalObjectStore
from ecomm.storage.s3 import S3ObjectStore

BACKEND_VAR = "ECOMM_S3_BACKEND"
BACKENDS = ("local", "minio")
DEFAULT_ENV_FILE = Path("docker/minio/.env")
DEFAULT_ENDPOINT = "http://127.0.0.1:9000"
LOCAL_S3_NAME = "s3-local"
MINIO_S3_NAME = "s3-minio"
GCS_NAME = "gcs-local"
_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")


@dataclass(frozen=True)
class LakeLayout:
    """Where the lake's local directories are. Pure: it creates nothing.

    A reader checks these BEFORE opening the stores, because opening a local store creates its
    directory; checked afterwards, "no lake yet" could never be detected.
    """

    s3_directory: Path | None  # None when the S3 side is a bucket (nothing to check on disk)
    local_directories: tuple[Path, ...]  # every local directory a full reader needs


@dataclass(frozen=True)
class LakeStores:
    """The lake's stores. ``destinations`` are the raw-zone copies that must stay in parity."""

    s3: ObjectStore
    gcs: ObjectStore
    control: ObjectStore

    @property
    def destinations(self) -> dict[str, ObjectStore]:
        return {self.s3.name: self.s3, self.gcs.name: self.gcs}


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    return values


def selected_backend(env: Mapping[str, str] | None = None) -> str:
    backend = (os.environ if env is None else env).get(BACKEND_VAR, "local").strip().lower()
    if backend not in BACKENDS:
        raise StorageConfigError(f"{BACKEND_VAR} must be one of {list(BACKENDS)}, got {backend!r}")
    return backend


def minio_store(
    env: Mapping[str, str] | None = None, env_file: Path = DEFAULT_ENV_FILE
) -> S3ObjectStore:
    """An S3 adapter pointed at MinIO, or an actionable error saying what is missing."""
    values = {**_read_env_file(env_file), **(os.environ if env is None else env)}
    missing = [
        key
        for key in ("ECOMM_S3_BUCKET", "ECOMM_S3_ACCESS_KEY", "ECOMM_S3_SECRET_KEY")
        if not values.get(key)
    ]
    if missing:
        raise StorageConfigError(
            f"{BACKEND_VAR}=minio needs {', '.join(missing)}. Create the random, git-ignored "
            "settings with: python docker/minio/generate_env.py"
        )
    bucket = values["ECOMM_S3_BUCKET"]
    if not _BUCKET_RE.fullmatch(bucket):
        raise StorageConfigError(f"ECOMM_S3_BUCKET {bucket!r} is not a valid bucket name")
    return S3ObjectStore(
        bucket,
        endpoint_url=values.get("ECOMM_S3_ENDPOINT_URL") or DEFAULT_ENDPOINT,
        access_key=values["ECOMM_S3_ACCESS_KEY"],
        secret_key=values["ECOMM_S3_SECRET_KEY"],
        name=MINIO_S3_NAME,
    )


def lake_layout(root: Path, env: Mapping[str, str] | None = None) -> LakeLayout:
    """The local directories the selected backend uses, without creating or opening anything."""
    return lake_layout_in(root / "lake", env)


def lake_layout_in(lake: Path, env: Mapping[str, str] | None = None) -> LakeLayout:
    """Like :func:`lake_layout`, for a lake directory itself (the Spark job's ``--lake-dir``)."""
    if selected_backend(env) == "minio":
        return LakeLayout(s3_directory=None, local_directories=(lake / GCS_NAME,))
    return LakeLayout(
        s3_directory=lake / LOCAL_S3_NAME,
        local_directories=(lake / LOCAL_S3_NAME, lake / GCS_NAME),
    )


def open_lake(
    root: Path,
    env: Mapping[str, str] | None = None,
    env_file: Path = DEFAULT_ENV_FILE,
) -> LakeStores:
    """Build the lake stores under ``root`` for the backend selected by the environment.

    Opening a local store creates its directory; use :func:`lake_layout` first to check that a
    lake exists.
    """
    return open_lake_in(root / "lake", env, env_file)


def open_lake_in(
    lake: Path,
    env: Mapping[str, str] | None = None,
    env_file: Path = DEFAULT_ENV_FILE,
) -> LakeStores:
    """Like :func:`open_lake`, for a lake directory itself (the Spark job's ``--lake-dir``)."""
    s3: ObjectStore
    if selected_backend(env) == "minio":
        s3 = minio_store(env, env_file)
    else:
        s3 = LocalObjectStore(lake / LOCAL_S3_NAME, name=LOCAL_S3_NAME)
    return LakeStores(
        s3=s3,
        gcs=LocalObjectStore(lake / GCS_NAME, name=GCS_NAME),
        control=LocalObjectStore(lake / "control", name="control"),
    )
