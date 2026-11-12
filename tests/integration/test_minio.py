"""The S3 adapter and the pipeline against a REAL S3-compatible server (MinIO).

Needs the compose ``minio`` profile up (``docker compose --profile minio up -d --build``) and the
generated secrets in ``docker/minio/``. Skipped otherwise, so a laptop without Docker stays green.

What this proves that the moto-based unit tests cannot:

* the adapter satisfies the storage contract over real HTTP (including "missing object" semantics);
* the pipeline works with **only** the permissions the Terraform pipeline role grants (the MinIO
  policy is the same statements), and nothing else is allowed;
* the whole batch path produces the same raw zone, byte for byte, as the local-directory backend,
  and the warehouse loaded from it holds the same events;
* replays write nothing new, and an unreachable endpoint degrades to PARTIAL in seconds, then heals.
"""

from __future__ import annotations

import contextlib
import json
import shutil
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import boto3
import duckdb
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from minio_support import purge_marker

from ecomm.demo import SOURCE, run_batch_demo
from ecomm.generator import GeneratorConfig, generate
from ecomm.lake.commit import BatchStatus, DestinationState, RetryPolicy, commit_batch
from ecomm.lake.ingest import classify
from ecomm.lake.parity import reconcile_all
from ecomm.lake.raw import prepare_batch
from ecomm.lake.sources import messages_from_emissions
from ecomm.lake.stores import BACKEND_VAR, DEFAULT_ENV_FILE, open_lake
from ecomm.storage.interface import ObjectNotFoundError, StorageError
from ecomm.storage.local import LocalObjectStore
from ecomm.storage.s3 import S3ObjectStore, client_options

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]
APP_ENV = REPO / DEFAULT_ENV_FILE
ROOT_ENV = REPO / "docker" / "minio" / ".env.root"


def read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip() and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                values[key.strip()] = value.strip()
    return values


@pytest.fixture(scope="module")
def env() -> dict[str, str]:
    values = read_env(APP_ENV)
    if not {"ECOMM_S3_BUCKET", "ECOMM_S3_ACCESS_KEY", "ECOMM_S3_SECRET_KEY"} <= set(values):
        pytest.skip("docker/minio/.env missing (python docker/minio/generate_env.py)")
    try:
        health = f"{values['ECOMM_S3_ENDPOINT_URL']}/minio/health/live"
        urllib.request.urlopen(health, timeout=5)  # noqa: S310 - loopback
    except OSError:
        pytest.skip("MinIO is not running (docker compose --profile minio up -d --build)")
    return {**values, BACKEND_VAR: "minio"}


@pytest.fixture(scope="module")
def root_client(env: dict[str, str]) -> Any:
    """Admin access, used only to prepare and clean up (the pipeline user cannot delete)."""
    root = read_env(ROOT_ENV)
    if "MINIO_ROOT_USER" not in root:
        pytest.skip("docker/minio/.env.root missing")
    return boto3.client(
        "s3",
        region_name="us-east-1",
        endpoint_url=env["ECOMM_S3_ENDPOINT_URL"],
        aws_access_key_id=root["MINIO_ROOT_USER"],
        aws_secret_access_key=root["MINIO_ROOT_PASSWORD"],
        config=Config(**client_options(env["ECOMM_S3_ENDPOINT_URL"])),
    )


def purge(client: Any, bucket: str, prefix: str) -> None:
    """Delete every version and delete marker under a prefix (the bucket is versioned)."""
    paginator = client.get_paginator("list_object_versions")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        found = [
            {"Key": v["Key"], "VersionId": v["VersionId"]}
            for v in page.get("Versions", []) + page.get("DeleteMarkers", [])
        ]
        if found:
            client.delete_objects(Bucket=bucket, Delete={"Objects": found})


def pipeline_store(env: dict[str, str], **overrides: str) -> S3ObjectStore:
    return S3ObjectStore(
        env["ECOMM_S3_BUCKET"],
        endpoint_url=overrides.get("endpoint", env["ECOMM_S3_ENDPOINT_URL"]),
        access_key=overrides.get("access_key", env["ECOMM_S3_ACCESS_KEY"]),
        secret_key=overrides.get("secret_key", env["ECOMM_S3_SECRET_KEY"]),
        name="s3-minio",
    )


@pytest.fixture
def prefix(env: dict[str, str], root_client: Any) -> Iterator[str]:
    value = f"raw/it-{uuid.uuid4().hex[:10]}/"
    yield value
    purge(root_client, env["ECOMM_S3_BUCKET"], value)


# --- the adapter's contract, over real HTTP -------------------------------------------------------


def test_round_trip_overwrite_list_and_binary_data(env: dict[str, str], prefix: str) -> None:
    store = pipeline_store(env)
    store.write_object(f"{prefix}a/b.jsonl", b"hello\n", {"batch_id": "b1"})
    assert store.read_object(f"{prefix}a/b.jsonl") == b"hello\n"
    assert store.object_exists(f"{prefix}a/b.jsonl")

    store.write_object(f"{prefix}k/one", b"first")
    store.write_object(f"{prefix}k/one", b"second")  # retries overwrite
    assert store.read_object(f"{prefix}k/one") == b"second"
    assert store.list_objects(f"{prefix}k/") == [f"{prefix}k/one"]

    for key in ("b/2", "a/1", "a/0"):
        store.write_object(f"{prefix}{key}", b"x")
    assert store.list_objects(f"{prefix}a/") == [
        f"{prefix}a/0",
        f"{prefix}a/1",
        f"{prefix}a/b.jsonl",
    ]
    assert store.list_objects(f"{prefix}zzz/") == []

    payload = bytes(range(256)) * 4096  # 1 MiB of every byte value
    store.write_object(f"{prefix}bin", payload)
    assert store.read_object(f"{prefix}bin") == payload


def test_keys_like_the_real_layout_survive_the_trip(env: dict[str, str], prefix: str) -> None:
    store = pipeline_store(env)
    key = f"{prefix}order_created/event_date=2026-11-01/event_hour=09/part-demo-42-b00000.jsonl"
    store.write_object(key, b"{}\n")
    assert store.list_objects(f"{prefix}order_created/") == [key]


def test_a_missing_object_is_not_found_not_an_error(env: dict[str, str], prefix: str) -> None:
    """The commit layer relies on this to decide a batch is new (see lake.commit)."""
    store = pipeline_store(env)
    assert store.object_exists(f"{prefix}nope/missing") is False
    with pytest.raises(ObjectNotFoundError):
        store.read_object(f"{prefix}nope/missing")


def minio_exec(*args: str, stdin: str | None = None) -> subprocess.CompletedProcess[str]:
    """Run `mc` inside the MinIO container as root (admin tasks: there is no S3 API for IAM)."""
    script = (
        'mc --config-dir /tmp/mc alias set l http://localhost:9000 "$MINIO_ROOT_USER" '
        '"$MINIO_ROOT_PASSWORD" >/dev/null && mc --config-dir /tmp/mc ' + " ".join(args)
    )
    return subprocess.run(  # noqa: S603 - fixed argv, no shell on the host
        [shutil.which("docker") or "docker", "exec", "-i", "ecomm-minio", "sh", "-c", script],
        input=stdin,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


@contextlib.contextmanager
def limited_user(
    env: dict[str, str], actions: list[str], *, list_prefix: bool
) -> Iterator[S3ObjectStore]:
    """A throw-away user that may put/get only what ``actions`` allow under raw/, created and removed
    through the admin client inside the MinIO container. ``list_prefix`` also grants a prefix list."""
    name, policy = f"probe{uuid.uuid4().hex[:8]}", f"probe-{uuid.uuid4().hex[:8]}"
    secret = uuid.uuid4().hex + uuid.uuid4().hex[:8]
    bucket = env["ECOMM_S3_BUCKET"]
    statements: list[dict[str, Any]] = [
        {"Effect": "Allow", "Action": actions, "Resource": [f"arn:aws:s3:::{bucket}/raw/*"]}
    ]
    if list_prefix:
        statements.append(
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": [f"arn:aws:s3:::{bucket}"],
                "Condition": {"StringLike": {"s3:prefix": ["raw/", "raw/*"]}},
            }
        )
    document = json.dumps({"Version": "2012-10-17", "Statement": statements})
    made = minio_exec("admin policy create l " + policy + " /dev/stdin", stdin=document)
    assert made.returncode == 0, made.stderr
    try:
        assert minio_exec(f"admin user add l {name} {secret}").returncode == 0
        attached = minio_exec(f"admin policy attach l {policy} --user {name}")
        assert attached.returncode == 0, attached.stderr
        yield pipeline_store(env, access_key=name, secret_key=secret)
    finally:
        minio_exec(f"admin user remove l {name}")
        minio_exec(f"admin policy remove l {policy}")


def test_a_key_that_exists_but_may_not_be_read_is_a_permission_error_never_missing(
    env: dict[str, str], prefix: str
) -> None:
    """A real 403 from a real server, for an object that EXISTS but the caller may not read.

    The adapter must not report it as missing: that would make a batch look new and overwrite
    committed data. Two outcomes of its probe are checked, both live: the probe is allowed and finds
    the key (the role may list but not read), and the probe is refused too (it cannot tell).

    What MinIO cannot reproduce is AWS's 403 for a MISSING key (this MinIO answers 404 even for a
    caller with no ListBucket); that branch is covered by the AWS-semantics double in the unit tests.
    """
    key = f"{prefix}guarded/manifest.json"
    pipeline_store(env).write_object(key, b"committed")  # put by the real pipeline user

    with limited_user(env, ["s3:PutObject"], list_prefix=True) as can_list_not_read:
        with pytest.raises(StorageError, match="exists but this role may not read") as raised:
            can_list_not_read.read_object(key)
        assert not isinstance(raised.value, ObjectNotFoundError)
        with pytest.raises(StorageError, match="exists but this role may not check"):
            can_list_not_read.object_exists(key)

    with limited_user(env, ["s3:PutObject"], list_prefix=False) as cannot_list:
        with pytest.raises(StorageError, match="prefix probe was refused") as raised:
            cannot_list.read_object(key)
        assert not isinstance(raised.value, ObjectNotFoundError)
        with pytest.raises(StorageError, match="permission denied"):
            cannot_list.object_exists(key)

    assert pipeline_store(env).read_object(key) == b"committed"  # nothing was changed by any of it


@pytest.mark.parametrize("key", ["", "/abs", "a/../b", "a//b", "a\\b", "./x"])
def test_invalid_keys_are_rejected_before_any_request(env: dict[str, str], key: str) -> None:
    with pytest.raises(ValueError, match="invalid object key"):
        pipeline_store(env).write_object(key, b"x")


# --- least privilege: the Terraform pipeline role, enforced by a real server ------------------------


def denied(call: Any, *args: Any, **kwargs: Any) -> str:
    with pytest.raises(ClientError) as raised:
        call(*args, **kwargs)
    return str(raised.value.response["Error"]["Code"])


def test_the_pipeline_user_can_do_its_job_and_nothing_more(
    env: dict[str, str], prefix: str, root_client: Any
) -> None:
    store = pipeline_store(env)
    client, bucket = store._client, env["ECOMM_S3_BUCKET"]  # noqa: SLF001
    client.put_object(Bucket=bucket, Key=f"{prefix}ok", Body=b"x")
    version = client.list_object_versions(Bucket=bucket, Prefix=f"{prefix}ok")["Versions"][0]

    # No delete, of an object or of one version of it: overwriting is the retry protocol.
    assert denied(client.delete_object, Bucket=bucket, Key=f"{prefix}ok") == "AccessDenied"
    assert (
        denied(
            client.delete_object, Bucket=bucket, Key=f"{prefix}ok", VersionId=version["VersionId"]
        )
        == "AccessDenied"
    )
    # Only the five pipeline prefixes: anything else is refused, for writes and for listing.
    assert denied(client.put_object, Bucket=bucket, Key="elsewhere/x", Body=b"x") == "AccessDenied"
    assert denied(client.list_objects_v2, Bucket=bucket) == "AccessDenied"
    assert denied(client.list_objects_v2, Bucket=bucket, Prefix="elsewhere/") == "AccessDenied"
    # No bucket administration at all.
    # MinIO answers ListBuckets with only the buckets the user may use (AWS answers AccessDenied).
    # Either way the pipeline user must see no bucket but its own, even when another one exists.
    other = f"ecomm-it-hidden-{uuid.uuid4().hex[:8]}"
    root_client.create_bucket(Bucket=other)
    try:
        visible = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
        assert visible <= {bucket} and other not in visible
    finally:
        root_client.delete_bucket(Bucket=other)
    assert denied(client.create_bucket, Bucket=f"ecomm-it-{uuid.uuid4().hex[:8]}") == "AccessDenied"
    assert (
        denied(
            client.put_bucket_versioning,
            Bucket=bucket,
            VersioningConfiguration={"Status": "Suspended"},
        )
        == "AccessDenied"
    )  # noqa: E501


def test_another_bucket_is_out_of_reach(env: dict[str, str], root_client: Any) -> None:
    other = f"ecomm-it-other-{uuid.uuid4().hex[:8]}"
    root_client.create_bucket(Bucket=other)
    try:
        root_client.put_object(Bucket=other, Key="raw/secret", Body=b"not yours")
        client = pipeline_store(env)._client  # noqa: SLF001
        assert denied(client.get_object, Bucket=other, Key="raw/secret") == "AccessDenied"
        assert denied(client.put_object, Bucket=other, Key="raw/x", Body=b"x") == "AccessDenied"
        assert denied(client.list_objects_v2, Bucket=other, Prefix="raw/") == "AccessDenied"
    finally:
        purge(root_client, other, "")
        root_client.delete_bucket(Bucket=other)


def test_the_bucket_is_private_and_versioned(
    env: dict[str, str], prefix: str, root_client: Any
) -> None:
    store = pipeline_store(env)
    store.write_object(f"{prefix}private", b"x")
    url = f"{env['ECOMM_S3_ENDPOINT_URL']}/{env['ECOMM_S3_BUCKET']}/{prefix}private"
    with pytest.raises(urllib.error.HTTPError) as anonymous:
        urllib.request.urlopen(url, timeout=10)  # noqa: S310 - loopback, no credentials
    assert anonymous.value.code == 403
    versioning = root_client.get_bucket_versioning(Bucket=env["ECOMM_S3_BUCKET"])
    assert versioning.get("Status") == "Enabled"  # like the Terraform bucket
    store.write_object(f"{prefix}private", b"y")
    kept = root_client.list_object_versions(
        Bucket=env["ECOMM_S3_BUCKET"], Prefix=f"{prefix}private"
    )
    assert len(kept["Versions"]) == 2  # the overwritten copy is kept


def test_wrong_credentials_fail_fast_and_never_echo_the_secret(
    env: dict[str, str], prefix: str
) -> None:
    secret = "definitely-the-wrong-secret"  # noqa: S105 - a deliberately invalid test value
    store = pipeline_store(env, secret_key=secret)
    started = time.monotonic()
    with pytest.raises(StorageError) as raised:
        store.write_object(f"{prefix}x", b"x")
    assert time.monotonic() - started < 20
    assert secret not in str(raised.value) and env["ECOMM_S3_SECRET_KEY"] not in str(raised.value)


# --- the whole pipeline on MinIO, compared with the local backend ---------------------------------


def demo_config() -> GeneratorConfig:
    start = GeneratorConfig().start
    return GeneratorConfig(
        seed=4242,
        orders=60,
        start=start,
        end=start + timedelta(days=2),
        malformed_rate=0.02,
        late_rate=0.02,
        out_of_order_rate=0.05,
        duplicate_rate=0.03,
    )


BATCHES = 2


@pytest.fixture(scope="module")
def runs(
    env: dict[str, str], root_client: Any, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[dict[str, Any]]:
    store = pipeline_store(env)
    foreign = [k for k in store.list_objects("manifests/") if f"source={SOURCE}/" in k]
    own = {f"batch_id=demo-4242-b{i:05d}/" for i in range(BATCHES)}
    if any(not any(part in key for part in own) for key in foreign):
        pytest.skip(
            "the bucket already holds other 'commerce' batches; the comparison needs a clean one "
            "(docker compose --profile minio down -v, then up again)"
        )
    base = tmp_path_factory.mktemp("minio-e2e")
    try:
        local = run_batch_demo(demo_config(), base / "local", BATCHES, env={})
        remote = run_batch_demo(demo_config(), base / "minio", BATCHES, env=env)
        yield {"local": local, "minio": remote, "base": base, "env": env}
    finally:
        # Leave the shared bucket as it was found: the next test that needs a clean one (and the
        # Airflow and exporter tests, which also write 'commerce' batches) must not skip because
        # of this run. Only this test's own batches are removed, matched by name.
        purge_marker(root_client, env["ECOMM_S3_BUCKET"], "demo-4242")


def test_every_batch_commits_to_minio_and_the_clouds_agree(runs: dict[str, Any]) -> None:
    summary = runs["minio"]
    assert summary["s3_backend"] == "s3-minio"
    assert [b["status"] for b in summary["batches"]] == ["COMPLETE"] * BATCHES
    for batch in summary["batches"]:
        assert set(batch["destinations"]) == {"s3-minio", "gcs-local"}
    assert summary["parity_ok"] is True and summary["warehouse_load_ok"] is True


def test_the_pipeline_counts_equal_the_local_backends(runs: dict[str, Any]) -> None:
    for key in (
        "valid_events",
        "duplicates_dropped",
        "quarantined",
        "late_events_kept",
        "warehouse_rows_inserted",
    ):
        assert runs["minio"][key] == runs["local"][key], key
    assert runs["minio"]["warehouse_rows_inserted"] > 0


def test_the_raw_zone_in_minio_is_byte_identical_to_the_local_one(runs: dict[str, Any]) -> None:
    base: Path = runs["base"]
    local = LocalObjectStore(base / "local" / "lake" / "s3-local")
    remote = open_lake(base / "minio", runs["env"]).s3
    local_keys = local.list_objects()
    assert local_keys, "the local run wrote nothing"
    minio_keys = [k for k in remote.list_objects("raw/") + remote.list_objects("manifests/")
                  + remote.list_objects("quarantine/") if f"source={SOURCE}/" in k or f"/{SOURCE}/" in k]  # fmt: skip
    assert sorted(minio_keys) == sorted(local_keys)
    for key in local_keys:
        assert remote.read_object(key) == local.read_object(key), key


def test_the_warehouse_loaded_from_minio_holds_the_same_events(runs: dict[str, Any]) -> None:
    def events(root: Path) -> list[tuple[Any, ...]]:
        con = duckdb.connect(str(root / "warehouse.duckdb"), read_only=True)
        try:
            return con.execute(
                "select event_type, count(*), count(distinct event_id) from raw.raw_events "
                "group by 1 order by 1"
            ).fetchall()
        finally:
            con.close()

    from_minio, from_local = events(runs["base"] / "minio"), events(runs["base"] / "local")
    assert from_minio == from_local and from_minio


def test_replaying_the_run_writes_nothing_new(runs: dict[str, Any]) -> None:
    base: Path = runs["base"]
    remote = open_lake(base / "minio", runs["env"]).s3
    before = {p: remote.list_objects(p) for p in ("raw/", "manifests/", "quarantine/")}
    again = run_batch_demo(demo_config(), base / "minio", BATCHES, env=runs["env"])
    assert {p: remote.list_objects(p) for p in before} == before
    assert again["warehouse_rows_inserted"] == 0  # the loader's MERGE found nothing new
    for batch in again["batches"]:
        assert all(d["already_committed"] for d in batch["destinations"].values())


def test_the_command_line_tools_follow_the_backend_setting(
    runs: dict[str, Any], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from ecomm.lake.reconcile_cli import main as reconcile
    from ecomm.warehouse.cli import main as load

    for key, value in runs["env"].items():
        monkeypatch.setenv(key, value)
    demo_dir = str(runs["base"] / "minio")
    assert reconcile(["--demo-dir", demo_dir, "--run-id", "it-reconcile"]) == 0
    assert "0 mismatched" in capsys.readouterr().out
    assert load(["--demo-dir", demo_dir, "--run-id", "it-load"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert sum(r["rows_inserted"] for r in report["results"]) == 0  # everything was loaded already
    lake = open_lake(runs["base"] / "minio", runs["env"])
    assert all(p.ok for p in reconcile_all(lake.destinations, SOURCE))


# --- failure: an unreachable endpoint, then healing ------------------------------------------------


def one_batch(source: str) -> Any:
    config = demo_config()
    messages = messages_from_emissions(generate(config).emissions)
    classified = classify(
        messages, set(), processed_at=config.end, late_threshold=timedelta(minutes=60)
    )
    return prepare_batch(
        classified.valid[:30],
        source=source,
        run_id="it-outage",
        batch_id="it-outage-b0",
        ingested_at=config.end,
    )


def test_an_unreachable_endpoint_is_partial_within_seconds_and_then_heals(
    env: dict[str, str], tmp_path: Path, root_client: Any
) -> None:
    source = f"itest-{uuid.uuid4().hex[:8]}"
    batch = one_batch(source)
    gcs = LocalObjectStore(tmp_path / "gcs", name="gcs-local")
    dead = pipeline_store(env, endpoint="http://127.0.0.1:1")  # nothing listens here
    fast = RetryPolicy(max_attempts=2, base_delay_s=0.0)
    try:
        started = time.monotonic()
        down = commit_batch(
            batch, {"s3-minio": dead, "gcs-local": gcs}, retry=fast, sleep=lambda _s: None
        )
        assert time.monotonic() - started < 30  # bounded: not botocore's default minute per try
        assert down.status is BatchStatus.PARTIAL
        assert down.destinations["s3-minio"].state is DestinationState.FAILED
        assert down.destinations["gcs-local"].state is DestinationState.COMMITTED

        live = pipeline_store(env)
        healed = commit_batch(batch, {"s3-minio": live, "gcs-local": gcs}, retry=fast)
        assert healed.status is BatchStatus.COMPLETE
        assert healed.destinations[
            "gcs-local"
        ].already_committed  # nothing rewritten on the good side
        assert live.list_objects(f"manifests/source={source}/") != []
    finally:
        for p in (f"raw/{source}/", f"manifests/source={source}/"):
            purge(root_client, env["ECOMM_S3_BUCKET"], p)
