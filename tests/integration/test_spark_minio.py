"""End to end: Kafka -> the real Spark container -> MinIO (the S3 side of the raw zone) -> warehouse.

Needs: Kafka (``docker compose up -d kafka``), MinIO with its secrets and bucket
(``docker compose --profile minio up -d --build`` + ``minio-init``, see docs/minio.md), the Spark image
built with ``boto3`` (``docker compose --profile spark build spark``) and Docker on PATH. Skipped
otherwise. It reuses the crash/restart scenario of ``test_spark.py`` with the S3 side on MinIO and
adds what only a network store can show:

* the container reaches MinIO as the least-privilege pipeline user (never the root user);
* with MinIO stopped the job fails in seconds, before Spark starts, and writes nothing anywhere;
* after MinIO is back, the restart completes and the result equals the pure-Python reference ingest;
* both destinations are byte-identical, and the warehouse loaded from MinIO has no double counting.

Isolation: a throw-away topic prefix, and a throw-away ``--source`` so every object key lands under
its own prefix in the shared bucket; everything is purged afterwards.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
import uuid
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import boto3
import pytest
from botocore.config import Config

from ecomm.generator import GeneratorConfig, generate
from ecomm.lake.ingest import ClassifiedBatch, classify
from ecomm.lake.manifest import BatchManifest, manifest_key
from ecomm.lake.parity import list_batch_ids, reconcile_all
from ecomm.lake.raw import event_ids_in_object
from ecomm.lake.sources import messages_from_emissions
from ecomm.lake.stats import list_ingest_stats
from ecomm.lake.stores import minio_store
from ecomm.producer.kafka_client import (
    NO_EXPIRY,
    check_health,
    create_producer,
    delete_topics,
    ensure_topics,
    expected_topics,
)
from ecomm.producer.publisher import EventPublisher, PublishItem, extract_event_id
from ecomm.storage.local import LocalObjectStore
from ecomm.storage.s3 import S3ObjectStore, client_options
from ecomm.warehouse.duckdb_client import DuckDBClient
from ecomm.warehouse.loader import WarehouseLoader

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]
BOOTSTRAP = "127.0.0.1:9092"
IMAGE = "ecomm-spark:3.5.6"
APP_ENV = REPO / "docker" / "minio" / ".env"
ROOT_ENV = REPO / "docker" / "minio" / ".env.root"
LATE = timedelta(minutes=60)
START = GeneratorConfig().start
CONFIG = GeneratorConfig(
    seed=41,
    orders=45,
    customers=10,
    products=6,
    end=START + timedelta(hours=18),
    malformed_rate=0.06,
    late_rate=0.06,
    out_of_order_rate=0.10,
    duplicate_rate=0.08,
)
# Generous: on a starved 5 GB machine one containerised run took many minutes.
SPARK_TIMEOUT_S = 1800
OUTAGE_BUDGET_S = 180  # the probe must fail far faster than a Spark start would take


def read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip() and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                values[key.strip()] = value.strip()
    return values


def docker(
    *args: str, timeout: int = 120, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    executable = shutil.which("docker") or "docker"
    return subprocess.run(  # noqa: S603 - fixed argv, no shell
        [executable, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=REPO,
        check=False,
        env={**os.environ, **(env or {})},
    )


def tail(result: subprocess.CompletedProcess[str]) -> str:
    return (result.stdout + result.stderr)[-3000:]


def purge(client: Any, bucket: str, prefix: str) -> None:
    """Delete every version and delete marker under a prefix (the bucket is versioned)."""
    for page in client.get_paginator("list_object_versions").paginate(Bucket=bucket, Prefix=prefix):
        found = [
            {"Key": v["Key"], "VersionId": v["VersionId"]}
            for v in page.get("Versions", []) + page.get("DeleteMarkers", [])
        ]
        if found:
            client.delete_objects(Bucket=bucket, Delete={"Objects": found})


@dataclass
class Scenario:
    prefix: str
    source: str
    host_dir: Path
    s3: S3ObjectStore
    oracle: ClassifiedBatch
    emissions_count: int
    crash: subprocess.CompletedProcess[str]
    batches_after_crash: list[str]
    outage: subprocess.CompletedProcess[str]
    outage_seconds: float
    gcs_before_outage: dict[str, bytes]
    gcs_after_outage: dict[str, bytes]
    resume: subprocess.CompletedProcess[str]
    rerun: subprocess.CompletedProcess[str]
    minio_after_resume: dict[str, bytes]
    minio_after_rerun: dict[str, bytes]

    def local(self, name: str) -> LocalObjectStore:
        return LocalObjectStore(self.host_dir / "lake" / name, name=name)


def own_keys(store: S3ObjectStore, source: str) -> list[str]:
    return [
        key
        for prefix in (
            f"raw/{source}/",
            f"manifests/source={source}/",
            f"quarantine/source={source}/",
        )
        for key in store.list_objects(prefix)
    ]


def snapshot_minio(store: S3ObjectStore, source: str) -> dict[str, bytes]:
    return {key: store.read_object(key) for key in own_keys(store, source)}


def snapshot_local(store: LocalObjectStore) -> dict[str, bytes]:
    return {key: store.read_object(key) for key in store.list_objects()}


def tree_snapshot(directory: Path) -> dict[str, bytes]:
    """Every file under a directory (sidecars included), reading only: nothing is created.

    The container (root) creates ``data/spark`` and everything under it; on Linux the host user then
    cannot create directories there, and ``LocalObjectStore`` creates its folder when constructed.
    Found in CI: constructing one before the container had run failed with PermissionError. So the
    host only ever reads what the container wrote, exactly like ``test_spark.py``.
    """
    if not directory.is_dir():
        return {}
    return {
        p.relative_to(directory).as_posix(): p.read_bytes()
        for p in sorted(directory.rglob("*"))
        if p.is_file()
    }


def wait_healthy(seconds: int = 120) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        state = docker("inspect", "-f", "{{.State.Health.Status}}", "ecomm-minio").stdout.strip()
        if state == "healthy":
            return
        time.sleep(3)
    raise AssertionError("MinIO did not become healthy again")


@pytest.fixture(scope="module")
def scenario() -> Iterator[Scenario]:
    if shutil.which("docker") is None:
        pytest.skip("docker is not on PATH")
    if docker("image", "inspect", IMAGE).returncode != 0:
        pytest.skip(f"image {IMAGE} not built (docker compose --profile spark build spark)")
    if docker("run", "--rm", "--entrypoint", "python", IMAGE, "-c", "import boto3").returncode != 0:
        pytest.skip(
            "the Spark image has no boto3: rebuild it (docker compose --profile spark build spark)"
        )
    if not check_health(BOOTSTRAP, timeout_s=3.0).ok:
        pytest.skip(f"no Kafka broker at {BOOTSTRAP} (docker compose up -d kafka)")
    app, root = read_env(APP_ENV), read_env(ROOT_ENV)
    if (
        not {"ECOMM_S3_BUCKET", "ECOMM_S3_ACCESS_KEY", "ECOMM_S3_SECRET_KEY"} <= set(app)
        or not root
    ):
        pytest.skip("MinIO secrets missing (python docker/minio/generate_env.py)")
    if (
        docker("inspect", "-f", "{{.State.Health.Status}}", "ecomm-minio").stdout.strip()
        != "healthy"
    ):
        pytest.skip("MinIO is not running (docker compose --profile minio up -d)")

    env = {"ECOMM_S3_BACKEND": "minio"}
    s3 = minio_store({**app, **env}, APP_ENV)
    root_client = boto3.client(
        "s3",
        region_name="us-east-1",
        endpoint_url=app["ECOMM_S3_ENDPOINT_URL"],
        aws_access_key_id=root["MINIO_ROOT_USER"],
        aws_secret_access_key=root["MINIO_ROOT_PASSWORD"],
        config=Config(**client_options(app["ECOMM_S3_ENDPOINT_URL"])),
    )
    prefix = f"spm{uuid.uuid4().hex[:8]}"
    source = f"spm-{uuid.uuid4().hex[:8]}"
    topics = expected_topics(prefix)
    ensure_topics(
        BOOTSTRAP, topics, partitions=3, retention_ms=NO_EXPIRY
    )  # old timestamps: no expiry
    host_dir = REPO / "data" / "spark" / "itest" / prefix

    def run_job(*extra: str) -> subprocess.CompletedProcess[str]:
        return docker(
            "compose", "--profile", "spark", "run", "--rm", "-T", "spark",
            "--topic-prefix", prefix,
            "--source", source,
            "--run-id", "itest",
            "--lake-dir", f"/data/itest/{prefix}/lake",
            "--checkpoint-dir", f"/data/itest/{prefix}/checkpoint",
            "--max-offsets-per-trigger", "100",
            *extra,
            timeout=SPARK_TIMEOUT_S,
            env=env,
        )  # fmt: skip

    try:
        emissions = generate(CONFIG).emissions
        publisher = EventPublisher(create_producer(BOOTSTRAP), flush_timeout_s=60)
        report = publisher.publish(
            PublishItem(
                f"{prefix}.{e.topic.split('.', 1)[1]}",
                e.key,
                e.value,
                extract_event_id(e.value),
                int(e.arrival_time.timestamp() * 1000),
            )
            for e in emissions
        )
        assert report.ok, report.to_json_dict()
        oracle = classify(
            messages_from_emissions(emissions), set(), processed_at=START, late_threshold=LATE
        )
        gcs_dir = host_dir / "lake" / "gcs-local"  # written by the container; the host only reads

        # 1. crash right after batch 1 was committed to BOTH MinIO and the local GCS copy
        crash = run_job("--fail-after-commit-batch", "1")
        batches_after_crash = list_batch_ids(s3, source)

        # 2. MinIO is down: the job must fail fast, before Spark starts, writing nothing
        gcs_before = tree_snapshot(gcs_dir)
        assert docker("compose", "--profile", "minio", "stop", "minio").returncode == 0
        try:
            started = time.monotonic()
            outage = run_job()
            outage_seconds = time.monotonic() - started
        finally:
            docker("compose", "--profile", "minio", "up", "-d", "--wait", "minio", timeout=300)
            wait_healthy()
        gcs_after = tree_snapshot(gcs_dir)

        # 3. MinIO is back: restart from the checkpoint; batch 1 is replayed, then the rest
        resume = run_job()
        after_resume = snapshot_minio(s3, source)
        # 4. nothing new to read
        rerun = run_job()
        after_rerun = snapshot_minio(s3, source)
        yield Scenario(
            prefix, source, host_dir, s3, oracle, len(emissions), crash, batches_after_crash,
            outage, outage_seconds, gcs_before, gcs_after, resume, rerun, after_resume, after_rerun,
        )  # fmt: skip
    finally:
        docker("compose", "--profile", "minio", "up", "-d", "--wait", "minio", timeout=300)
        delete_topics(BOOTSTRAP, topics)
        shutil.rmtree(host_dir, ignore_errors=True)
        for p in (f"raw/{source}/", f"manifests/source={source}/", f"quarantine/source={source}/"):
            purge(root_client, app["ECOMM_S3_BUCKET"], p)


# --- the wiring: which credentials and which endpoint the container sees ---------------------------


def test_the_container_gets_the_pipeline_user_and_the_in_network_endpoint_never_root() -> None:
    result = docker(
        "compose", "--profile", "spark", "run", "--rm", "-T", "--entrypoint", "env", "spark",
        env={"ECOMM_S3_BACKEND": "minio"},
    )  # fmt: skip
    assert result.returncode == 0, tail(result)
    seen = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    app = read_env(APP_ENV)
    assert seen["ECOMM_S3_BACKEND"] == "minio"
    assert seen["ECOMM_S3_ENDPOINT_URL"] == "http://minio:9000"  # a host name, not 127.0.0.1
    assert seen["ECOMM_S3_ACCESS_KEY"] == app["ECOMM_S3_ACCESS_KEY"]
    assert not [k for k in seen if k.startswith("MINIO_ROOT")]  # the root login stays out
    root = read_env(ROOT_ENV)
    assert root["MINIO_ROOT_PASSWORD"] not in result.stdout


def test_without_the_setting_the_container_still_uses_local_directories() -> None:
    result = docker(
        "compose", "--profile", "spark", "run", "--rm", "-T", "--entrypoint", "env", "spark"
    )
    assert result.returncode == 0, tail(result)
    assert "ECOMM_S3_BACKEND=local" in result.stdout.splitlines()


# --- the crash, the outage and the restart ---------------------------------------------------------


def test_the_injected_crash_happened_after_batch_one_reached_minio(scenario: Scenario) -> None:
    assert scenario.crash.returncode != 0, tail(scenario.crash)
    output = scenario.crash.stdout + scenario.crash.stderr
    assert "injected failure after committing batch 1" in output
    assert '"destinations":["gcs-local","s3-minio"]' in output.replace(" ", "")
    assert scenario.batches_after_crash == ["itest-b00000", "itest-b00001"]  # read from MinIO


def test_with_minio_down_the_job_fails_fast_and_writes_nothing(scenario: Scenario) -> None:
    output = scenario.outage.stdout + scenario.outage.stderr
    assert scenario.outage.returncode == 1, tail(scenario.outage)
    assert "destination unreachable; not starting Spark" in output
    assert "destination s3-minio is not reachable" in output
    assert "starting query" not in output  # Spark never started
    assert scenario.outage_seconds < OUTAGE_BUDGET_S
    assert scenario.gcs_before_outage, "the GCS copy was empty: the comparison below proves nothing"
    assert scenario.gcs_after_outage == scenario.gcs_before_outage  # no half-written side
    secret = read_env(APP_ENV)["ECOMM_S3_SECRET_KEY"]
    assert secret not in output  # credentials never reach the logs


def test_restart_after_the_outage_completes_successfully(scenario: Scenario) -> None:
    assert scenario.resume.returncode == 0, tail(scenario.resume)
    assert "query finished" in scenario.resume.stdout + scenario.resume.stderr


def test_rerun_with_nothing_new_changes_nothing_in_minio(scenario: Scenario) -> None:
    assert scenario.rerun.returncode == 0, tail(scenario.rerun)
    assert scenario.minio_after_rerun == scenario.minio_after_resume
    assert "batch committed" not in scenario.rerun.stdout + scenario.rerun.stderr


# --- the result equals the reference ingest ---------------------------------------------------------


def landed_ids(scenario: Scenario) -> list[str]:
    s3 = scenario.s3
    return [
        i
        for key in s3.list_objects(f"raw/{scenario.source}/")
        for i in event_ids_in_object(s3.read_object(key))
    ]


def test_landed_events_equal_the_reference_valid_set_without_duplicates(scenario: Scenario) -> None:
    ids = landed_ids(scenario)
    expected = {str(e.envelope.event_id) for e in scenario.oracle.valid}
    assert len(ids) == len(set(ids)), "an event was written twice (restart double counting)"
    assert set(ids) == expected


def test_statistics_equal_the_reference_and_survive_the_replay(scenario: Scenario) -> None:
    stats = list_ingest_stats(scenario.local("control"), scenario.source)
    oracle = scenario.oracle
    assert sum(s.input_count for s in stats) == scenario.emissions_count
    assert sum(s.valid for s in stats) == len(oracle.valid)
    assert sum(s.duplicates for s in stats) == len(oracle.duplicates)
    assert sum(s.quarantined for s in stats) == len(oracle.quarantined)
    assert sum(s.late for s in stats) == oracle.late_count
    assert len(oracle.duplicates) > 0 and len(oracle.quarantined) > 0 and oracle.late_count > 0


def test_quarantine_in_minio_holds_every_invalid_message_with_its_reason(
    scenario: Scenario,
) -> None:
    s3 = scenario.s3
    records = [
        json.loads(line)
        for key in s3.list_objects(f"quarantine/source={scenario.source}/")
        for line in s3.read_object(key).decode().splitlines()
    ]
    expected = Counter(r.error_code for r in scenario.oracle.quarantined)
    assert Counter(r["error_code"] for r in records) == expected
    assert all(r["original_payload"] and r["topic"].startswith(scenario.prefix) for r in records)


def test_late_flags_in_the_raw_files_match_the_reference(scenario: Scenario) -> None:
    s3 = scenario.s3
    late = {
        json.loads(line)["event"]["event_id"]
        for key in s3.list_objects(f"raw/{scenario.source}/")
        for line in s3.read_object(key).decode().splitlines()
        if json.loads(line)["ingest"]["is_late"]
    }
    assert late == {str(e.envelope.event_id) for e in scenario.oracle.valid if e.is_late}


def test_both_destinations_hold_identical_committed_batches(scenario: Scenario) -> None:
    minio = snapshot_minio(scenario.s3, scenario.source)
    gcs = snapshot_local(scenario.local("gcs-local"))
    assert minio and minio == gcs  # raw, manifests and quarantine: byte-identical across the two
    reports = reconcile_all(
        {"s3-minio": scenario.s3, "gcs-local": scenario.local("gcs-local")}, scenario.source
    )
    assert reports and all(r.ok for r in reports)
    for batch_id in list_batch_ids(scenario.s3, scenario.source):
        manifest = BatchManifest.from_bytes(
            scenario.s3.read_object(manifest_key(scenario.source, batch_id))
        )
        assert manifest.run_id == "itest" and manifest.source_offsets


def test_kafka_offsets_are_recorded_per_batch_and_cover_every_message(scenario: Scenario) -> None:
    control = scenario.local("control")
    spans: dict[str, list[tuple[int, int]]] = {}
    for key in control.list_objects("spark/"):
        data = json.loads(control.read_object(key))
        for tp, (lo, hi) in data["offsets"].items():
            spans.setdefault(tp, []).append((lo, hi))
    covered = 0
    for tp, ranges in spans.items():
        ranges.sort()
        for (_, prev_hi), (next_lo, _) in zip(ranges, ranges[1:], strict=False):
            assert next_lo == prev_hi + 1, f"{tp}: gap or overlap between batches"
        covered += sum(hi - lo + 1 for lo, hi in ranges)
    assert covered == scenario.emissions_count


# --- all the way to the warehouse: no double counting in business totals -----------------------------


def test_warehouse_loaded_from_minio_equals_the_reference_and_a_replay_adds_nothing(
    scenario: Scenario, tmp_path: Path
) -> None:
    client = DuckDBClient(tmp_path / "w.duckdb")
    try:
        loader = WarehouseLoader(client, scenario.s3, scenario.source)
        window = (START.date(), (START + timedelta(days=2)).date())
        first = loader.backfill(*window, "spark-minio-1")
        assert first.ok and sum(r.rows_inserted for r in first.results) == len(
            scenario.oracle.valid
        )
        replay = loader.backfill(*window, "spark-minio-2")
        assert replay.ok and sum(r.rows_inserted for r in replay.results) == 0
        total = client.query(
            "SELECT SUM(CAST(json_extract_string(payload, '$.total_amount') AS DECIMAL(18,2))) AS s "
            f"FROM {client.ref('raw_events')} WHERE event_type = 'order_created'"
        )[0]["s"]
    finally:
        client.close()
    expected_total = sum(
        (
            Decimal(json.loads(e.message.value)["payload"]["total_amount"])
            for e in scenario.oracle.valid
            if e.envelope.event_type.value == "order_created"
        ),
        Decimal(0),
    )
    assert total == expected_total > 0
