"""Behavioural contract every ObjectStore adapter must satisfy."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import boto3
import pytest
from gcs_fakes import FakeGCSClient
from moto import mock_aws

from ecomm.storage import (
    LocalObjectStore,
    ObjectNotFoundError,
    ObjectStore,
    StorageConfigError,
    validate_key,
)
from ecomm.storage.gcs import GCSObjectStore
from ecomm.storage.s3 import S3ObjectStore
from ecomm.storage.testing import InMemoryObjectStore


@pytest.fixture(params=["local", "memory", "s3-moto", "gcs-fake"])
def store(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[ObjectStore]:
    kind = request.param
    if kind == "local":
        yield LocalObjectStore(tmp_path / "bucket")
    elif kind == "memory":
        yield InMemoryObjectStore()
    elif kind == "s3-moto":
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="test-bucket")
            yield S3ObjectStore("test-bucket", client=client)
    else:
        yield GCSObjectStore("test-bucket", client=FakeGCSClient())


def test_write_then_read_round_trip(store: ObjectStore) -> None:
    store.write_object("raw/a/b.jsonl", b"hello\n", {"batch_id": "b1"})
    assert store.read_object("raw/a/b.jsonl") == b"hello\n"
    assert store.object_exists("raw/a/b.jsonl")


def test_write_overwrites_so_retries_are_safe(store: ObjectStore) -> None:
    store.write_object("k/one", b"first")
    store.write_object("k/one", b"second")
    assert store.read_object("k/one") == b"second"
    assert store.list_objects("k/") == ["k/one"]


def test_missing_object(store: ObjectStore) -> None:
    assert not store.object_exists("nope/missing")
    with pytest.raises(ObjectNotFoundError):
        store.read_object("nope/missing")


def test_list_is_prefix_filtered_and_sorted(store: ObjectStore) -> None:
    for key in ("raw/b/2", "raw/a/1", "other/x", "raw/a/0"):
        store.write_object(key, b"x")
    assert store.list_objects("raw/") == ["raw/a/0", "raw/a/1", "raw/b/2"]
    assert store.list_objects("zzz/") == []
    assert len(store.list_objects()) == 4


def test_binary_data_is_preserved(store: ObjectStore) -> None:
    payload = bytes(range(256)) * 4
    store.write_object("bin/data", payload)
    assert store.read_object("bin/data") == payload


@pytest.mark.parametrize("key", ["", "/abs", "a/../b", "a//b", "a\\b", "./x", "a/."])
def test_invalid_keys_are_rejected(store: ObjectStore, key: str) -> None:
    with pytest.raises(ValueError, match="invalid object key"):
        store.write_object(key, b"x")


def test_validate_key_accepts_normal_keys() -> None:
    assert validate_key("raw/orders/event_date=2026-11-01/part-1.jsonl")


def test_local_store_metadata_and_traversal(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path)
    store.write_object("a/b", b"x", {"batch_id": "b1"})
    assert store.read_metadata("a/b") == {"batch_id": "b1"}
    assert store.list_objects() == ["a/b"]  # sidecar hidden
    with pytest.raises(ValueError, match="invalid object key"):
        store.write_object("../escape", b"x")


def test_s3_adapter_requires_bucket() -> None:
    with pytest.raises(StorageConfigError):
        S3ObjectStore("", client=object())


def test_gcs_adapter_requires_bucket() -> None:
    with pytest.raises(StorageConfigError):
        GCSObjectStore("", client=FakeGCSClient())


def test_gcs_without_credentials_gives_actionable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    monkeypatch.setenv("GCE_METADATA_HOST", "127.0.0.1:1")  # make the metadata probe fail fast
    monkeypatch.setenv("HOME", "/nonexistent")
    monkeypatch.setenv("APPDATA", "/nonexistent")
    with pytest.raises(StorageConfigError, match="credentials"):
        GCSObjectStore("bucket")
