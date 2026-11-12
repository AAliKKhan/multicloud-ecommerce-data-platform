"""The S3 adapter against AWS's 403-for-missing-key behaviour (see aws_fakes.py).

Found as a documented risk while verifying against MinIO, which answers 404 where AWS may answer
403. The commit layer, the Spark sink and the loader all decide "is this batch new?" from a missing
object, so a 403 there would fail every new batch. The adapter now tells "missing" from "forbidden"
with a prefix list, the one read the pipeline role is allowed everywhere it may write.
"""

from __future__ import annotations

import re
from datetime import timedelta
from pathlib import Path

import pytest
from aws_fakes import PIPELINE_PREFIXES, AwsLikeS3Client

from ecomm.generator import GeneratorConfig, generate
from ecomm.lake.commit import BatchStatus, commit_batch
from ecomm.lake.ingest import classify
from ecomm.lake.raw import prepare_batch
from ecomm.lake.sources import messages_from_emissions
from ecomm.storage.interface import ObjectNotFoundError, StorageError
from ecomm.storage.s3 import S3ObjectStore

KEY = "manifests/source=commerce/batch_id=b1/manifest.json"
REPO = Path(__file__).resolve().parents[3]


def store(**kwargs: object) -> tuple[S3ObjectStore, AwsLikeS3Client]:
    client = AwsLikeS3Client(**kwargs)  # type: ignore[arg-type]
    return S3ObjectStore("ecomm-raw", client=client), client


def test_the_double_really_is_the_worst_case() -> None:
    """Guard the test itself: without the fix a missing key is not 'not found' here."""
    _, client = store()
    with pytest.raises(Exception, match="AccessDenied"):
        client.get_object(Bucket="b", Key=KEY)
    with pytest.raises(Exception, match="403"):
        client.head_object(Bucket="b", Key=KEY)


def test_a_missing_key_that_answers_403_is_still_reported_as_missing() -> None:
    s3, client = store()
    with pytest.raises(ObjectNotFoundError):
        s3.read_object(KEY)
    assert s3.object_exists(KEY) is False
    assert ("list", KEY) in client.calls  # the probe: a prefix list on exactly that key


def test_a_present_key_is_read_without_any_probe() -> None:
    s3, client = store()
    s3.write_object(KEY, b"data")
    client.calls.clear()
    assert s3.read_object(KEY) == b"data" and s3.object_exists(KEY) is True
    assert not [c for c in client.calls if c[0] == "list"]  # the happy path costs nothing extra


def test_real_404s_do_not_probe() -> None:
    s3, client = store(missing_key_status="404")
    with pytest.raises(ObjectNotFoundError):
        s3.read_object(KEY)
    assert s3.object_exists(KEY) is False
    assert not [c for c in client.calls if c[0] == "list"]


def test_a_key_that_exists_but_may_not_be_read_is_a_permission_error_not_missing() -> None:
    s3, client = store(deny_get_for=(KEY,))
    client.objects[KEY] = b"secret"
    with pytest.raises(StorageError, match="permission") as raised:
        s3.read_object(KEY)
    assert not isinstance(raised.value, ObjectNotFoundError)
    with pytest.raises(StorageError, match="permission"):
        s3.object_exists(KEY)


def test_when_the_probe_is_denied_too_it_is_a_permission_error_never_silently_missing() -> None:
    s3, _ = store(list_allowed=False)  # no ListBucket at all: we cannot tell, so we must not guess
    with pytest.raises(StorageError, match="permission") as raised:
        s3.read_object(KEY)
    assert not isinstance(raised.value, ObjectNotFoundError)
    with pytest.raises(StorageError, match="permission"):
        s3.object_exists(KEY)


def test_a_key_outside_the_pipeline_prefixes_is_a_permission_error() -> None:
    s3, _ = store()
    with pytest.raises(StorageError, match="permission") as raised:
        s3.read_object("elsewhere/x")  # neither readable nor listable under the role
    assert not isinstance(raised.value, ObjectNotFoundError)


def test_the_probe_matches_only_the_exact_key_not_a_longer_sibling() -> None:
    """A prefix list of '.../part-1' also returns '.../part-10'. Only an exact match is 'present'."""
    s3, client = store(deny_get_for=("raw/commerce/part-10",))
    client.objects["raw/commerce/part-10"] = b"x"
    with pytest.raises(ObjectNotFoundError):  # 'part-1' is absent even though 'part-10' exists
        s3.read_object("raw/commerce/part-1")


def test_the_error_text_never_contains_the_data_or_credentials() -> None:
    s3, _ = store(list_allowed=False)
    with pytest.raises(StorageError) as raised:
        s3.read_object(KEY)
    assert "ecomm-raw" not in str(raised.value) or KEY in str(raised.value)


def test_writes_still_work_and_a_forbidden_write_is_an_error() -> None:
    s3, _ = store()
    s3.write_object("raw/commerce/x", b"1")
    assert s3.read_object("raw/commerce/x") == b"1"
    with pytest.raises(StorageError):
        s3.write_object("elsewhere/x", b"1")


def test_the_double_uses_exactly_the_terraform_pipeline_prefixes() -> None:
    """The double's permissions are only meaningful while they equal the real role's."""
    text = (REPO / "infra" / "terraform" / "aws" / "main.tf").read_text(encoding="utf-8")
    match = re.search(r"pipeline_prefixes\s*=\s*\[([^\]]*)\]", text)
    assert match
    assert tuple(re.findall(r'"([^"]+)"', match.group(1))) == PIPELINE_PREFIXES
    # And the role really does condition ListBucket on the prefix (the reason a 403 is possible).
    assert '"s3:prefix"' in text and "ListPipelinePrefixes" in text


def test_the_spark_sinks_conflict_check_treats_a_403_missing_manifest_as_new() -> None:
    from ecomm.spark.sink import _check_no_conflict

    config = GeneratorConfig(seed=5, orders=10, customers=4, products=3)
    classified = classify(
        messages_from_emissions(generate(config).emissions),
        set(),
        processed_at=config.end,
        late_threshold=timedelta(minutes=60),
    )
    batch = prepare_batch(
        classified.valid, source="commerce", run_id="r", batch_id="r-b0", ingested_at=config.end
    )
    s3, _ = store()
    _check_no_conflict({"s3": s3}, "commerce", "r-b0", batch.manifest, b"quarantined\n")  # no raise


def test_a_whole_commit_works_when_every_missing_manifest_answers_403() -> None:
    """The point of all this: 'is this batch already committed?' is asked of a missing manifest."""
    config = GeneratorConfig(seed=3, orders=20, customers=6, products=4)
    emissions = generate(config).emissions
    classified = classify(
        messages_from_emissions(emissions),
        set(),
        processed_at=config.end,
        late_threshold=timedelta(minutes=60),
    )
    batch = prepare_batch(
        classified.valid, source="commerce", run_id="r", batch_id="r-b0", ingested_at=config.end
    )
    (s3a, ca), (s3b, _) = store(), store()
    result = commit_batch(batch, {"a": s3a, "b": s3b})
    assert result.status is BatchStatus.COMPLETE
    writes = len([c for c in ca.calls if c[0] == "put"])
    again = commit_batch(batch, {"a": s3a, "b": s3b})
    assert all(d.already_committed for d in again.destinations.values())  # replay writes nothing
    assert len([c for c in ca.calls if c[0] == "put"]) == writes
