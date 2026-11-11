"""Spark integration tests: run the real containerised streaming job against Kafka.

Needs: ``docker compose up -d kafka``, the image built (``docker compose --profile spark
build spark``) and Docker on PATH. Skipped automatically otherwise. One scenario runs the
job three times (crash right after a commit, restart, nothing-new rerun) and every test
asserts a different property of the same outcome, always against the pure-Python reference
ingest (:func:`ecomm.lake.ingest.classify`).

Everything is isolated under a throw-away topic prefix and ``data/spark/itest/<prefix>/``.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import uuid
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from ecomm.generator import GeneratorConfig, generate
from ecomm.lake.ingest import ClassifiedBatch, classify
from ecomm.lake.manifest import BatchManifest, manifest_key
from ecomm.lake.parity import list_batch_ids, reconcile_all
from ecomm.lake.raw import event_ids_in_object
from ecomm.lake.sources import messages_from_emissions
from ecomm.lake.stats import list_ingest_stats
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
from ecomm.warehouse.duckdb_client import DuckDBClient
from ecomm.warehouse.loader import WarehouseLoader

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]
BOOTSTRAP = "127.0.0.1:9092"
IMAGE = "ecomm-spark:3.5.6"
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
# Generous: on a starved 8 GB machine one containerised run took over 15 minutes.
SPARK_TIMEOUT_S = 1800


def docker(*args: str, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    executable = shutil.which("docker") or "docker"
    return subprocess.run(  # noqa: S603 - fixed argv, no shell
        [executable, *args], capture_output=True, text=True, timeout=timeout, cwd=REPO, check=False
    )


@dataclass
class Scenario:
    prefix: str
    host_dir: Path
    oracle: ClassifiedBatch
    emissions_count: int
    crash: subprocess.CompletedProcess[str]
    resume: subprocess.CompletedProcess[str]
    rerun: subprocess.CompletedProcess[str]
    batches_after_crash: list[str]
    objects_after_resume: dict[str, bytes]
    objects_after_rerun: dict[str, bytes]

    @property
    def lake(self) -> Path:
        return self.host_dir / "lake"

    def store(self, name: str) -> LocalObjectStore:
        return LocalObjectStore(self.lake / name, name=name)


def run_job(prefix: str, *extra: str) -> subprocess.CompletedProcess[str]:
    return docker(
        "compose", "--profile", "spark", "run", "--rm", "-T", "spark",
        "--topic-prefix", prefix,
        "--run-id", "itest",
        "--lake-dir", f"/data/itest/{prefix}/lake",
        "--checkpoint-dir", f"/data/itest/{prefix}/checkpoint",
        "--max-offsets-per-trigger", "100",
        *extra,
        timeout=SPARK_TIMEOUT_S,
    )  # fmt: skip


def snapshot(store: LocalObjectStore) -> dict[str, bytes]:
    return {key: store.read_object(key) for key in store.list_objects()}


@pytest.fixture(scope="module")
def scenario() -> Iterator[Scenario]:
    if shutil.which("docker") is None:
        pytest.skip("docker is not on PATH")
    if docker("image", "inspect", IMAGE).returncode != 0:
        pytest.skip(f"image {IMAGE} not built (docker compose --profile spark build spark)")
    if not check_health(BOOTSTRAP, timeout_s=3.0).ok:
        pytest.skip(f"no Kafka broker at {BOOTSTRAP} (docker compose up -d kafka)")

    prefix = f"spk{uuid.uuid4().hex[:8]}"
    topics = expected_topics(prefix)
    # Simulated timestamps are old: the topics must never expire mid-test.
    ensure_topics(BOOTSTRAP, topics, partitions=3, retention_ms=NO_EXPIRY)
    host_dir = REPO / "data" / "spark" / "itest" / prefix
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

        # 1. crash right after batch 1 has been committed (before Spark checkpoints it)
        crash = run_job(prefix, "--fail-after-commit-batch", "1")
        lake = LocalObjectStore(host_dir / "lake" / "s3-local", name="s3-local")
        batches_after_crash = list_batch_ids(lake, "commerce")
        # 2. restart from the checkpoint: batch 1 is replayed, then the rest is processed
        resume = run_job(prefix)
        objects_after_resume = {
            n: snapshot(LocalObjectStore(host_dir / "lake" / n, name=n))
            for n in ("s3-local", "gcs-local")
        }
        # 3. nothing new to read
        rerun = run_job(prefix)
        objects_after_rerun = {
            n: snapshot(LocalObjectStore(host_dir / "lake" / n, name=n))
            for n in ("s3-local", "gcs-local")
        }
        yield Scenario(
            prefix, host_dir, oracle, len(emissions), crash, resume, rerun,
            batches_after_crash, objects_after_resume, objects_after_rerun,
        )  # fmt: skip
    finally:
        delete_topics(BOOTSTRAP, topics)
        shutil.rmtree(host_dir, ignore_errors=True)


def tail(result: subprocess.CompletedProcess[str]) -> str:
    return (result.stdout + result.stderr)[-3000:]


# --- the crash and the restart ------------------------------------------------------------------


def test_the_injected_crash_happened_after_batch_one_was_committed(scenario: Scenario) -> None:
    assert scenario.crash.returncode != 0, tail(scenario.crash)
    assert (
        "injected failure after committing batch 1" in scenario.crash.stdout + scenario.crash.stderr
    )
    assert scenario.batches_after_crash == ["itest-b00000", "itest-b00001"]


def test_restart_completes_successfully(scenario: Scenario) -> None:
    assert scenario.resume.returncode == 0, tail(scenario.resume)
    assert "query finished" in scenario.resume.stdout + scenario.resume.stderr


def test_rerun_with_nothing_new_changes_nothing(scenario: Scenario) -> None:
    assert scenario.rerun.returncode == 0, tail(scenario.rerun)
    assert scenario.objects_after_rerun == scenario.objects_after_resume
    assert "batch committed" not in scenario.rerun.stdout + scenario.rerun.stderr


# --- the result equals the reference ingest ----------------------------------------------------------


def landed_ids(scenario: Scenario) -> list[str]:
    s3 = scenario.store("s3-local")
    return [i for key in s3.list_objects("raw/") for i in event_ids_in_object(s3.read_object(key))]


def test_landed_events_equal_the_reference_valid_set_without_duplicates(scenario: Scenario) -> None:
    ids = landed_ids(scenario)
    expected = {str(e.envelope.event_id) for e in scenario.oracle.valid}
    assert len(ids) == len(set(ids)), "an event was written twice (restart double counting)"
    assert set(ids) == expected


def test_statistics_equal_the_reference_and_survive_the_replay(scenario: Scenario) -> None:
    stats = list_ingest_stats(scenario.store("control"), "commerce")
    oracle = scenario.oracle
    assert sum(s.input_count for s in stats) == scenario.emissions_count
    assert sum(s.valid for s in stats) == len(oracle.valid)
    assert sum(s.duplicates for s in stats) == len(oracle.duplicates)
    assert sum(s.quarantined for s in stats) == len(oracle.quarantined)
    assert sum(s.late for s in stats) == oracle.late_count
    assert len(oracle.duplicates) > 0 and len(oracle.quarantined) > 0 and oracle.late_count > 0


def test_late_flags_in_the_raw_files_match_the_reference(scenario: Scenario) -> None:
    s3 = scenario.store("s3-local")
    late = {
        json.loads(line)["event"]["event_id"]
        for key in s3.list_objects("raw/")
        for line in s3.read_object(key).decode().splitlines()
        if json.loads(line)["ingest"]["is_late"]
    }
    assert late == {str(e.envelope.event_id) for e in scenario.oracle.valid if e.is_late}


def test_quarantine_holds_every_invalid_message_with_its_reason(scenario: Scenario) -> None:
    s3 = scenario.store("s3-local")
    records = [
        json.loads(line)
        for key in s3.list_objects("quarantine/")
        for line in s3.read_object(key).decode().splitlines()
    ]
    expected = Counter(r.error_code for r in scenario.oracle.quarantined)
    assert Counter(r["error_code"] for r in records) == expected
    assert all(r["original_payload"] and r["topic"].startswith(scenario.prefix) for r in records)


def test_both_destinations_hold_identical_committed_batches(scenario: Scenario) -> None:
    s3, gcs = scenario.store("s3-local"), scenario.store("gcs-local")
    assert snapshot(s3) == snapshot(gcs)  # raw, manifests and quarantine: byte-identical
    assert all(r.ok for r in reconcile_all({"s3-local": s3, "gcs-local": gcs}, "commerce"))
    for batch_id in list_batch_ids(s3, "commerce"):
        manifest = BatchManifest.from_bytes(s3.read_object(manifest_key("commerce", batch_id)))
        assert manifest.run_id == "itest" and manifest.source_offsets


def test_kafka_offsets_are_recorded_per_batch_and_cover_every_message(scenario: Scenario) -> None:
    s3 = scenario.store("s3-local")
    spans: dict[str, list[tuple[int, int]]] = {}
    for key in scenario.store("control").list_objects("spark/"):
        data = json.loads(scenario.store("control").read_object(key))
        for tp, (lo, hi) in data["offsets"].items():
            spans.setdefault(tp, []).append((lo, hi))
    covered = 0
    for tp, ranges in spans.items():
        ranges.sort()
        for (_, prev_hi), (next_lo, _) in zip(ranges, ranges[1:], strict=False):
            assert next_lo == prev_hi + 1, f"{tp}: gap or overlap between batches"
        covered += sum(hi - lo + 1 for lo, hi in ranges)
    assert covered == scenario.emissions_count
    assert s3.list_objects("manifests/")


# --- regression: a watermark that is too short must FAIL the job, never lose messages quietly -------------


def test_too_short_a_retention_fails_loudly_instead_of_dropping_messages_silently() -> None:
    """Before the offset-plan check, Spark discarded rows older than its watermark and the job
    reported success with fewer messages than Kafka held (15 of 1490 in the real run)."""
    if shutil.which("docker") is None or docker("image", "inspect", IMAGE).returncode != 0:
        pytest.skip("docker or the Spark image is not available")
    if not check_health(BOOTSTRAP, timeout_s=3.0).ok:
        pytest.skip(f"no Kafka broker at {BOOTSTRAP}")

    prefix = f"wmk{uuid.uuid4().hex[:8]}"
    topics = expected_topics(prefix)
    ensure_topics(BOOTSTRAP, topics, partitions=3, retention_ms=NO_EXPIRY)
    host_dir = REPO / "data" / "spark" / "itest" / prefix
    try:
        publisher = EventPublisher(create_producer(BOOTSTRAP), flush_timeout_s=60)
        report = publisher.publish(
            PublishItem(
                f"{prefix}.{e.topic.split('.', 1)[1]}",
                e.key,
                e.value,
                extract_event_id(e.value),
                int(e.arrival_time.timestamp() * 1000),
            )
            for e in generate(CONFIG).emissions
        )
        assert report.ok
        # A watermark of ~4 seconds of event time: later batches contain rows older than it.
        result = run_job(prefix, "--dedup-retention-hours", "0.001")
        output = result.stdout + result.stderr
        assert result.returncode == 1, tail(result)  # a clean failure exit, not a crash trace
        failure = next(ln for ln in output.splitlines() if '"msg":"query failed"' in ln)
        assert "OffsetGapError" in failure and "lost messages between Kafka" in failure, failure
        lake = LocalObjectStore(host_dir / "lake" / "s3-local", name="s3-local")
        committed = list_batch_ids(lake, "commerce")
        # Batches committed before the failing one are complete and contiguous from 0.
        assert committed == [f"itest-b{i:05d}" for i in range(len(committed))]
        gcs = LocalObjectStore(host_dir / "lake" / "gcs-local", name="gcs-local")
        reports = reconcile_all({"s3-local": lake, "gcs-local": gcs}, "commerce")
        assert all(r.ok for r in reports)  # whatever was committed is consistent on both sides
    finally:
        delete_topics(BOOTSTRAP, topics)
        shutil.rmtree(host_dir, ignore_errors=True)


# --- all the way to the warehouse: no double counting in business totals --------------------------------


def test_warehouse_totals_equal_the_reference_and_a_replay_adds_nothing(
    scenario: Scenario, tmp_path: Path
) -> None:
    s3 = scenario.store("s3-local")
    client = DuckDBClient(tmp_path / "w.duckdb")
    try:
        loader = WarehouseLoader(client, s3, "commerce")
        first = loader.backfill(START.date(), (START + timedelta(days=2)).date(), "spark-1")
        assert first.ok and sum(r.rows_inserted for r in first.results) == len(
            scenario.oracle.valid
        )

        replay = loader.backfill(START.date(), (START + timedelta(days=2)).date(), "spark-2")
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
