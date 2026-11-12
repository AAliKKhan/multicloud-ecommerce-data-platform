"""The four Airflow DAGs, run by real Airflow against a raw zone that lives in MinIO.

Needs MinIO with its secrets (``docker compose --profile minio up -d --build`` + ``minio-init``) and
the Airflow image built with the S3 client (``docker compose --profile airflow build airflow-init``).
Skipped otherwise. ``airflow dags test`` runs in a container on the compose network, as the
least-privilege pipeline user (never root), exactly like the local-folder smoke test in CI.

What it proves:

* every DAG runs to success when the S3 side of the raw zone is a bucket (``check_lake_ready`` read
  it, the load MERGEd from it, dbt and the quality suite ran on the result, the daily reconciliation
  compared MinIO with the GCS copy);
* the DAGs only READ the raw zone: every object in the bucket is byte-identical afterwards;
* the warehouse the DAGs built holds exactly the events that were committed, each once;
* an unreachable bucket makes the readiness check FAIL (exit 1) and never skip (exit 99): a skipped
  run looks healthy, so an outage reported as "nothing to load" would go unnoticed.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import duckdb
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

from ecomm.demo import run_batch_demo
from ecomm.generator import GeneratorConfig
from ecomm.lake.stores import BACKEND_VAR
from ecomm.storage.s3 import S3ObjectStore

pytestmark = pytest.mark.integration

IMAGE = "ecomm-airflow:2.10.5"
VENV_PYTHON = "/home/airflow/ecomm-venv/bin/python"
SEED = 4343
MARKER = f"demo-{SEED}"
BATCHES = 3
SMOKE_TIMEOUT_S = 3000  # four DAGs including dbt on a 5 GB machine


def demo_config() -> GeneratorConfig:
    start = GeneratorConfig().start
    return GeneratorConfig(
        seed=SEED,
        orders=120,
        start=start,
        end=start + timedelta(days=3),
        malformed_rate=0.02,
        late_rate=0.02,
        out_of_order_rate=0.05,
        duplicate_rate=0.03,
    )


def network_of(container: str) -> str:
    result = docker(
        "inspect",
        "-f",
        "{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}",
        container,
    )
    return result.stdout.strip()


def snapshot(store: S3ObjectStore) -> dict[str, bytes]:
    keys = [
        key
        for prefix in ("raw/", "manifests/", "quarantine/")
        for key in store.list_objects(prefix)
        if MARKER in key
    ]
    return {key: store.read_object(key) for key in keys}


@dataclass
class Run:
    data_dir: Path
    summary: dict[str, Any]
    network: str
    env_file: Path
    store: S3ObjectStore
    before: dict[str, bytes]
    after: dict[str, bytes]
    smoke_returncode: int
    smoke_output: str


def container_env_file(path: Path, app: dict[str, str]) -> Path:
    """The pipeline user's settings for the container, in a file (not on the command line)."""
    lines = [
        f"{BACKEND_VAR}=minio",
        f"ECOMM_S3_BUCKET={app['ECOMM_S3_BUCKET']}",
        f"ECOMM_S3_ACCESS_KEY={app['ECOMM_S3_ACCESS_KEY']}",
        f"ECOMM_S3_SECRET_KEY={app['ECOMM_S3_SECRET_KEY']}",
        "ECOMM_S3_ENDPOINT_URL=http://minio:9000",  # a host name inside the compose network
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    if sys.platform != "win32":
        path.chmod(0o600)
    return path


def container_args(run_dir: Path, network: str, env_file: Path, data_dir: Path) -> list[str]:
    user = ["--user", f"{os.getuid()}:0"] if sys.platform != "win32" else []
    return [
        "run", "--rm", *user, "--network", network, "--env-file", str(env_file),
        "-e", "AIRFLOW__CORE__LOAD_EXAMPLES=false",
        "-e", "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/smoke.db",
        "-e", "AIRFLOW__SCARF_ANALYTICS__SCARF_ANALYTICS=false",
        "-v", f"{REPO / 'airflow' / 'dags'}:/opt/airflow/dags:ro",
        "-v", f"{REPO / 'src'}:/opt/ecomm/src:ro",
        "-v", f"{REPO / 'dbt'}:/opt/ecomm/dbt:ro",
        "-v", f"{REPO / 'scripts' / 'airflow_dags_smoke.sh'}:/smoke.sh:ro",
        "-v", f"{data_dir}:/data/pipeline",
    ]  # fmt: skip


@pytest.fixture(scope="module")
def run(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Run]:
    if shutil.which("docker") is None:
        pytest.skip("docker is not on PATH")
    if docker("image", "inspect", IMAGE).returncode != 0:
        pytest.skip(
            f"image {IMAGE} not built (docker compose --profile airflow build airflow-init)"
        )
    probe = docker("run", "--rm", "--entrypoint", VENV_PYTHON, IMAGE, "-c", "import boto3")
    if probe.returncode != 0:
        pytest.skip(
            "the Airflow image has no boto3: rebuild it (docker compose --profile airflow build)"
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
    network = network_of("ecomm-minio")
    data_dir = REPO / "data" / "airflow-minio" / uuid.uuid4().hex[:8]
    env_file = container_env_file(tmp_path_factory.mktemp("airflow-minio") / "pipeline.env", app)
    cleaner = root_client(app, root)
    try:
        # The raw zone, written by the pipeline itself: S3 side in MinIO, GCS and control local.
        summary = run_batch_demo(
            demo_config(), data_dir, BATCHES, env={**app, BACKEND_VAR: "minio"}
        )
        (data_dir / "warehouse.duckdb").unlink()  # the DAGs must load and model it themselves
        before = snapshot(store)
        smoke = docker(
            *container_args(data_dir, network, env_file, data_dir),
            "--entrypoint", "bash", IMAGE, "/smoke.sh",
            timeout=SMOKE_TIMEOUT_S,
        )  # fmt: skip
        yield Run(
            data_dir, summary, network, env_file, store, before, snapshot(store),
            smoke.returncode, smoke.stdout + smoke.stderr,
        )  # fmt: skip
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)
        purge_marker(cleaner, app["ECOMM_S3_BUCKET"], MARKER)


def tail(run: Run) -> str:
    return run.smoke_output[-3500:]


# --- the four DAGs against a bucket ------------------------------------------------------------------


def test_every_dag_runs_to_success_against_the_bucket(run: Run) -> None:
    assert run.smoke_returncode == 0, tail(run)
    assert "ALL_DAGS_OK" in run.smoke_output and "DAG_FAILED" not in run.smoke_output, tail(run)


def test_the_readiness_check_really_read_the_bucket(run: Run) -> None:
    assert "committed batches present in s3-minio" in run.smoke_output, tail(run)
    assert run.summary["s3_backend"] == "s3-minio" and run.before  # there was something to find


def test_the_dags_only_read_the_raw_zone(run: Run) -> None:
    assert run.before and run.after == run.before  # every object byte-identical afterwards


def test_the_warehouse_the_dags_built_holds_each_committed_event_once(run: Run) -> None:
    con = duckdb.connect(str(run.data_dir / "warehouse.duckdb"), read_only=True)
    try:
        total, distinct = con.execute(
            "select count(*), count(distinct event_id) from raw.raw_events"
        ).fetchone()  # type: ignore[misc]
    finally:
        con.close()
    assert total == distinct == run.summary["valid_events"] > 0


def test_the_reconciliation_compared_minio_with_the_gcs_copy(run: Run) -> None:
    reports = sorted(
        (run.data_dir / "lake" / "control" / "reports" / "reconciliation").glob("*.json")
    )
    assert reports, "the reconciliation DAG wrote no report"
    report = json.loads(reports[-1].read_text(encoding="utf-8"))
    assert report["ok"] is True and report["batches_checked"] == BATCHES
    destinations = {name for batch in report["batches"] for name in batch["destinations"]}
    assert destinations == {"s3-minio", "gcs-local"}


# --- an unreachable bucket must fail the readiness check, never skip it ------------------------------


def ready_check(run: Run, network: str) -> Any:
    return docker(
        *container_args(run.data_dir, network, run.env_file, run.data_dir),
        "-e", "PYTHONPATH=/opt/ecomm/src",
        "--entrypoint", VENV_PYTHON, IMAGE,
        "-m", "ecomm.lake.ready_cli", "--demo-dir", "/data/pipeline",
        timeout=300,
    )  # fmt: skip


def test_the_readiness_check_succeeds_inside_the_container_when_the_bucket_is_reachable(
    run: Run,
) -> None:
    result = ready_check(run, run.network)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "committed batches present in s3-minio" in result.stdout


def test_an_unreachable_bucket_fails_the_readiness_check_and_never_skips_it(run: Run) -> None:
    result = ready_check(run, "none")  # the bucket's host name cannot be resolved
    output = result.stdout + result.stderr
    assert result.returncode == 1, output  # 1 = fail; 99 would have SKIPPED the run silently
    assert "cannot list the committed batches of s3-minio" in output
    assert read_env(APP_ENV)["ECOMM_S3_SECRET_KEY"] not in output  # never leaked to the logs
