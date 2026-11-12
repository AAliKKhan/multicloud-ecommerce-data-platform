"""The metrics exporter container reading a raw zone that lives in MinIO.

Needs MinIO with its secrets and the exporter image built with the S3 client
(``docker compose --profile monitoring build ecomm-exporter``). Skipped otherwise. A throw-away
exporter container is started on the compose network with a published loopback port, so the test
scrapes it exactly as Prometheus would, without needing the whole monitoring stack.

What it proves:

* the numbers that come from the bucket are real: ``ecomm_events_committed`` is summed from the
  manifests read over S3, and the cross-cloud parity check compares the bucket with the GCS copy;
* a refresh against the bucket is cheap enough for a 60-second cadence (measured, reported);
* the exporter reads and never writes (the bucket is byte-identical afterwards);
* when MinIO goes away the exporter keeps serving and says which sections failed
  (``ecomm_scrape_errors``), and it recovers on its own when MinIO is back.
"""

from __future__ import annotations

import json
import shutil
import socket
import time
import urllib.request
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest
from minio_support import (
    APP_ENV,
    REPO,
    ROOT_ENV,
    docker,
    foreign_commerce_batches,
    minio_ready,
    pipeline_store,
    purge_marker,
    read_env,
    root_client,
)
from prometheus_client.parser import text_string_to_metric_families

from ecomm.demo import run_batch_demo
from ecomm.generator import GeneratorConfig
from ecomm.lake.stores import BACKEND_VAR
from ecomm.storage.s3 import S3ObjectStore

pytestmark = pytest.mark.integration

IMAGE = "ecomm-exporter:1"
SEED = 4444
MARKER = f"demo-{SEED}"
BATCHES = 2
REFRESH_S = 5


def demo_config() -> GeneratorConfig:
    start = GeneratorConfig().start
    return GeneratorConfig(
        seed=SEED,
        orders=60,
        start=start,
        end=start + timedelta(days=2),
        malformed_rate=0.02,
        late_rate=0.02,
        out_of_order_rate=0.05,
        duplicate_rate=0.03,
    )


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def scrape(port: int) -> dict[str, list[tuple[dict[str, str], float]]]:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=15) as response:  # noqa: S310
        text = response.read().decode()
    families: dict[str, list[tuple[dict[str, str], float]]] = {}
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            families.setdefault(sample.name, []).append((dict(sample.labels), sample.value))
    return families


def value(metrics: dict[str, Any], name: str, **labels: str) -> float | None:
    for sample_labels, sample_value in metrics.get(name, []):
        if all(sample_labels.get(k) == v for k, v in labels.items()):
            return float(sample_value)
    return None


def wait_for(condition: Any, seconds: float, what: str) -> Any:
    deadline = time.monotonic() + seconds
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = condition()
            if result:
                return result
        except OSError as exc:
            last = exc
        time.sleep(2)
    raise AssertionError(f"timed out waiting for {what}" + (f" ({last})" if last else ""))


def wait_minio_healthy(seconds: int = 120) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        state = docker("inspect", "-f", "{{.State.Health.Status}}", "ecomm-minio").stdout.strip()
        if state == "healthy":
            return
        time.sleep(3)
    raise AssertionError("MinIO did not become healthy again")


def snapshot(store: S3ObjectStore) -> dict[str, bytes]:
    keys = [
        key
        for prefix in ("raw/", "manifests/", "quarantine/")
        for key in store.list_objects(prefix)
        if MARKER in key
    ]
    return {key: store.read_object(key) for key in keys}


@dataclass
class Exporter:
    port: int
    summary: dict[str, Any]
    store: S3ObjectStore
    before: dict[str, bytes]
    first: dict[str, Any]


@pytest.fixture(scope="module")
def exporter(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Exporter]:
    if shutil.which("docker") is None:
        pytest.skip("docker is not on PATH")
    if docker("image", "inspect", IMAGE).returncode != 0:
        pytest.skip(
            f"image {IMAGE} not built (docker compose --profile monitoring build ecomm-exporter)"
        )
    if docker("run", "--rm", "--entrypoint", "python", IMAGE, "-c", "import boto3").returncode != 0:
        pytest.skip(
            "the exporter image has no boto3: rebuild it (docker compose --profile monitoring build)"
        )
    reason = minio_ready()
    if reason:
        pytest.skip(reason)
    app, root = read_env(APP_ENV), read_env(ROOT_ENV)
    store = pipeline_store(app)
    if foreign_commerce_batches(store, MARKER):
        pytest.skip(
            "the bucket already holds other 'commerce' batches; the check needs a clean one "
            "(docker compose --profile minio down -v, then up and init again)"
        )
    network = docker(
        "inspect",
        "-f",
        "{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}",
        "ecomm-minio",
    ).stdout.strip()
    data_dir = REPO / "data" / "exporter-minio" / uuid.uuid4().hex[:8]
    env_file = tmp_path_factory.mktemp("exporter-minio") / "pipeline.env"
    env_file.write_text(
        "\n".join(
            [
                f"{BACKEND_VAR}=minio",
                f"ECOMM_S3_BUCKET={app['ECOMM_S3_BUCKET']}",
                f"ECOMM_S3_ACCESS_KEY={app['ECOMM_S3_ACCESS_KEY']}",
                f"ECOMM_S3_SECRET_KEY={app['ECOMM_S3_SECRET_KEY']}",
                "ECOMM_S3_ENDPOINT_URL=http://minio:9000",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    port, name = free_port(), f"ecomm-exporter-itest-{uuid.uuid4().hex[:6]}"
    cleaner = root_client(app, root)
    try:
        summary = run_batch_demo(
            demo_config(), data_dir, BATCHES, env={**app, BACKEND_VAR: "minio"}
        )
        before = snapshot(store)
        started = docker(
            "run", "-d", "--name", name, "--network", network, "-p", f"127.0.0.1:{port}:9108",
            "--env-file", str(env_file),
            "-v", f"{REPO / 'src'}:/app/src:ro",
            "-v", f"{data_dir}:/data/pipeline:ro",
            IMAGE,
            # 0.0.0.0 is inside the container; the host side is published to loopback only (-p).
            "--demo-dir", "/data/pipeline", "--serve", "9108", "--addr", "0.0.0.0",  # noqa: S104
            "--refresh-seconds", str(REFRESH_S),
        )  # fmt: skip
        assert started.returncode == 0, started.stderr
        first = wait_for(
            lambda: (
                (m := scrape(port)) and value(m, "ecomm_exporter_ready", reason="") == 1.0 and m
            ),
            240,
            "the exporter's first snapshot",
        )
        yield Exporter(port, summary, store, before, first)
    finally:
        logs = docker("logs", "--tail", "15", name)
        if logs.returncode == 0:
            print((logs.stdout + logs.stderr)[-1500:])
        docker("rm", "-f", name)
        docker("compose", "--profile", "minio", "up", "-d", "--wait", "minio", timeout=300)
        shutil.rmtree(data_dir, ignore_errors=True)
        purge_marker(cleaner, app["ECOMM_S3_BUCKET"], MARKER)


def test_the_exporter_reads_the_bucket_and_reports_what_was_committed(exporter: Exporter) -> None:
    metrics = exporter.first
    committed = value(metrics, "ecomm_events_committed")
    assert committed == exporter.summary["valid_events"] > 0  # summed from manifests read over S3
    assert value(metrics, "ecomm_parity_batches_checked") == BATCHES
    assert value(metrics, "ecomm_parity_batches_mismatched") == 0  # MinIO and the GCS copy agree
    assert value(metrics, "ecomm_batches", status="COMPLETE") == BATCHES


def test_nothing_is_reported_as_failing_while_the_bucket_is_healthy(exporter: Exporter) -> None:
    errors = {
        labels["section"]: v for labels, v in exporter.first.get("ecomm_scrape_errors", []) if v
    }
    assert errors == {}, errors


def test_a_refresh_against_the_bucket_is_cheap_enough_for_a_minute_cadence(
    exporter: Exporter,
) -> None:
    seconds = value(exporter.first, "ecomm_exporter_refresh_duration_seconds")
    print(f"refresh against MinIO took {seconds:.2f}s for {BATCHES} batches")
    assert seconds is not None and 0 < seconds < 30  # a scrape never waits for it (cached)


def test_the_exporter_never_writes_to_the_bucket(exporter: Exporter) -> None:
    assert exporter.before and snapshot(exporter.store) == exporter.before


def test_when_minio_goes_away_the_exporter_keeps_serving_and_names_the_failing_sections(
    exporter: Exporter,
) -> None:
    assert docker("compose", "--profile", "minio", "stop", "minio").returncode == 0
    try:

        def degraded() -> Any:
            metrics = scrape(exporter.port)  # still answers: the scrape itself must not fail
            failing = {
                labels["section"]
                for labels, v in metrics.get("ecomm_scrape_errors", [])
                if v and labels.get("section") in ("lake", "parity")
            }
            return metrics if failing == {"lake", "parity"} else None

        metrics = wait_for(
            degraded, 180, "the exporter to report the lake and parity sections as failing"
        )
        assert value(metrics, "ecomm_exporter_snapshot_age_seconds") is not None  # still serving
    finally:
        docker("compose", "--profile", "minio", "up", "-d", "--wait", "minio", timeout=300)
        wait_minio_healthy()

    def recovered() -> Any:
        metrics = scrape(exporter.port)
        clean = not any(v for _, v in metrics.get("ecomm_scrape_errors", []))
        return (
            metrics if clean and value(metrics, "ecomm_parity_batches_checked") == BATCHES else None
        )

    metrics = wait_for(recovered, 240, "the exporter to recover once MinIO is back")
    assert value(metrics, "ecomm_events_committed") == exporter.summary["valid_events"]
    assert json.dumps(sorted(exporter.before)) == json.dumps(sorted(snapshot(exporter.store)))
