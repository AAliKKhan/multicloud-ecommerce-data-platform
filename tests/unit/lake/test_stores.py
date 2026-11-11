"""The lake-store factory: which backend the S3 side uses, and what happens when it is misconfigured."""

from __future__ import annotations

from pathlib import Path

import pytest

from ecomm.lake.stores import (
    BACKEND_VAR,
    LOCAL_S3_NAME,
    MINIO_S3_NAME,
    lake_layout,
    minio_store,
    open_lake,
    selected_backend,
)
from ecomm.storage.interface import StorageConfigError
from ecomm.storage.local import LocalObjectStore
from ecomm.storage.s3 import S3ObjectStore

MINIO_ENV = {
    BACKEND_VAR: "minio",
    "ECOMM_S3_BUCKET": "ecomm-raw-minio",
    "ECOMM_S3_ACCESS_KEY": "pipeline-user",
    "ECOMM_S3_SECRET_KEY": "pipeline-secret-value",
}
NO_FILE = Path("does-not-exist.env")


def test_the_default_backend_is_the_local_directory_so_nothing_changes_unless_asked(
    tmp_path: Path,
) -> None:
    lake = open_lake(tmp_path, {})
    assert isinstance(lake.s3, LocalObjectStore) and lake.s3.name == LOCAL_S3_NAME
    assert set(lake.destinations) == {"s3-local", "gcs-local"}
    layout = lake_layout(tmp_path, {})
    assert layout.s3_directory == tmp_path / "lake" / "s3-local"
    assert layout.local_directories == (
        tmp_path / "lake" / "s3-local",
        tmp_path / "lake" / "gcs-local",
    )


def test_the_minio_backend_uses_the_s3_adapter_and_says_where_it_lives(tmp_path: Path) -> None:
    lake = open_lake(tmp_path, MINIO_ENV, NO_FILE)
    assert isinstance(lake.s3, S3ObjectStore)
    assert lake.s3.name == MINIO_S3_NAME == "s3-minio"  # never mistaken for AWS
    assert lake.s3.bucket == "ecomm-raw-minio"
    assert set(lake.destinations) == {"s3-minio", "gcs-local"}
    layout = lake_layout(tmp_path, MINIO_ENV)
    assert layout.s3_directory is None  # a bucket has no directory to check
    assert layout.local_directories == (tmp_path / "lake" / "gcs-local",)
    # The GCS side and the control store stay local in both modes.
    assert isinstance(lake.gcs, LocalObjectStore) and isinstance(lake.control, LocalObjectStore)


def test_the_layout_creates_nothing_but_opening_the_stores_does(tmp_path: Path) -> None:
    lake_layout(tmp_path, {})
    assert not (tmp_path / "lake").exists()
    open_lake(tmp_path, {})
    # This is why a reader must check the layout BEFORE opening: afterwards it always exists.
    assert (tmp_path / "lake" / "s3-local").is_dir()


def test_every_reader_reports_a_missing_lake_and_does_not_create_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ecomm.lake.quarantine_cli import main as quarantine
    from ecomm.lake.reconcile_cli import main as reconcile
    from ecomm.warehouse.cli import main as load

    monkeypatch.delenv(BACKEND_VAR, raising=False)
    nowhere = tmp_path / "none"
    for tool in (quarantine, reconcile, load):
        assert tool(["--demo-dir", str(nowhere)]) == 2
    assert not nowhere.exists()  # a typo in --demo-dir must not leave an empty lake behind


@pytest.mark.parametrize("value", ["", "  LOCAL ", "Local"])
def test_backend_names_are_case_and_space_insensitive(value: str) -> None:
    assert selected_backend({BACKEND_VAR: value} if value else {}) == "local"


def test_an_unknown_backend_is_an_error_not_a_silent_fallback() -> None:
    with pytest.raises(StorageConfigError, match=r"must be one of \['local', 'minio'\]"):
        selected_backend({BACKEND_VAR: "s4"})


@pytest.mark.parametrize(
    "missing", ["ECOMM_S3_BUCKET", "ECOMM_S3_ACCESS_KEY", "ECOMM_S3_SECRET_KEY"]
)
def test_missing_minio_settings_are_named_with_the_fix(missing: str) -> None:
    env = {k: v for k, v in MINIO_ENV.items() if k != missing}
    with pytest.raises(StorageConfigError, match=missing) as raised:
        minio_store(env, NO_FILE)
    assert "generate_env.py" in str(raised.value)


def test_an_invalid_bucket_name_is_rejected_early() -> None:
    with pytest.raises(StorageConfigError, match="not a valid bucket name"):
        minio_store({**MINIO_ENV, "ECOMM_S3_BUCKET": "Bad_Bucket"}, NO_FILE)


def test_settings_come_from_the_env_file_and_the_process_environment_wins(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "# generated\nECOMM_S3_BUCKET=from-file\nECOMM_S3_ACCESS_KEY=file-user\n"
        "ECOMM_S3_SECRET_KEY=file-secret-1\nECOMM_S3_ENDPOINT_URL=http://file-host:9000\n",
        encoding="utf-8",
    )
    from_file = minio_store({BACKEND_VAR: "minio"}, env_file)
    assert from_file.bucket == "from-file"
    overridden = minio_store({BACKEND_VAR: "minio", "ECOMM_S3_BUCKET": "from-env-bucket"}, env_file)
    assert overridden.bucket == "from-env-bucket"


def test_the_endpoint_defaults_to_loopback_and_credentials_are_not_put_in_the_environment() -> None:
    import os

    before = dict(os.environ)
    store = minio_store(MINIO_ENV, NO_FILE)
    assert store._client.meta.endpoint_url == "http://127.0.0.1:9000"  # noqa: SLF001
    assert dict(os.environ) == before  # nothing leaked into the process environment


def test_a_custom_endpoint_is_used() -> None:
    store = minio_store({**MINIO_ENV, "ECOMM_S3_ENDPOINT_URL": "http://minio:9000"}, NO_FILE)
    assert store._client.meta.endpoint_url == "http://minio:9000"  # noqa: SLF001
