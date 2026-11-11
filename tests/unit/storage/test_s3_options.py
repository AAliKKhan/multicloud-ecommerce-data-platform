"""How the S3 adapter configures its client for self-hosted endpoints. No network is used."""

from __future__ import annotations

import pytest

from ecomm.storage.interface import StorageConfigError
from ecomm.storage.s3 import (
    BOTO_MAX_ATTEMPTS,
    CONNECT_TIMEOUT_S,
    READ_TIMEOUT_S,
    S3ObjectStore,
    client_options,
)


def test_timeouts_are_bounded_for_every_endpoint() -> None:
    for endpoint in (None, "http://127.0.0.1:9000"):
        options = client_options(endpoint)
        assert options["connect_timeout"] == CONNECT_TIMEOUT_S <= 10
        assert options["read_timeout"] == READ_TIMEOUT_S <= 60
        assert options["retries"]["max_attempts"] == BOTO_MAX_ATTEMPTS <= 3


def test_real_aws_keeps_the_sdk_defaults_for_addressing_and_checksums() -> None:
    options = client_options(None)
    assert "s3" not in options and "request_checksum_calculation" not in options


def test_a_custom_endpoint_uses_path_style_and_only_required_checksums() -> None:
    options = client_options("http://minio:9000")
    assert options["s3"] == {"addressing_style": "path"}
    assert options["request_checksum_calculation"] == "when_required"
    assert options["response_checksum_validation"] == "when_required"


def test_the_client_really_receives_those_settings() -> None:
    store = S3ObjectStore(
        "ecomm-raw-minio",
        endpoint_url="http://127.0.0.1:9000",
        access_key="user-name",
        secret_key="dummy-not-a-real-secret",  # noqa: S106 - test value, never sent anywhere
    )
    config = store._client.meta.config  # noqa: SLF001
    assert config.s3["addressing_style"] == "path"
    assert config.connect_timeout == CONNECT_TIMEOUT_S
    assert store._client.meta.region_name == "us-east-1"  # noqa: SLF001 - needed even for MinIO


def test_one_half_of_a_key_pair_is_an_error() -> None:
    with pytest.raises(StorageConfigError, match="both an access key and a secret key"):
        S3ObjectStore("bucket", endpoint_url="http://x:9000", access_key="only-one")
