"""Airflow integration tests: parse the DAGs with the REAL Airflow, in a throw-away container.

Needs Docker and the image (``docker compose --profile airflow build airflow-init``); skipped
otherwise. A container is started with an in-memory-ish SQLite metadata DB (no Postgres, no secrets,
nothing persistent), the DAG folder mounted read-only, and ``DagBag`` is asked to load everything.
This checks what static tests cannot: that Airflow itself accepts the DAGs (imports, parameters,
dataset scheduling, operator arguments) and that the policy settings really ended up on the objects.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]
IMAGE = "ecomm-airflow:2.10.5"

INSPECT = r"""
import json
from datetime import timedelta
from airflow.models import DagBag

bag = DagBag("/opt/airflow/dags", include_examples=False)
out = {"errors": {k: v[-300:] for k, v in bag.import_errors.items()}, "dags": {}}
for dag_id, dag in bag.dags.items():
    out["dags"][dag_id] = {
        "schedule": str(dag.timetable.summary),
        "catchup": dag.catchup,
        "max_active_runs": dag.max_active_runs,
        "max_active_tasks": dag.max_active_tasks,
        "dagrun_timeout_s": dag.dagrun_timeout.total_seconds() if dag.dagrun_timeout else None,
        "paused_at_creation": dag.get_is_paused_at_creation() if hasattr(dag, "get_is_paused_at_creation") else None,
        "params": sorted(dag.params.keys()),
        "edges": sorted(f"{a}>{b}" for a, b in ((t.task_id, d) for t in dag.tasks for d in t.downstream_task_ids)),
        "tasks": {
            t.task_id: {
                "retries": t.retries,
                "backoff": t.retry_exponential_backoff,
                "max_retry_delay_s": t.max_retry_delay.total_seconds() if t.max_retry_delay else None,
                "timeout_s": t.execution_timeout.total_seconds() if t.execution_timeout else None,
                "pool": t.pool,
                "outlets": [o.uri for o in t.outlets],
                "has_failure_callback": bool(t.on_failure_callback),
                "type": type(t).__name__,
            }
            for t in dag.tasks
        },
    }
print("RESULT=" + json.dumps(out))
"""


def docker(*args: str, timeout: int = 600) -> subprocess.CompletedProcess[str]:
    executable = shutil.which("docker") or "docker"
    return subprocess.run(  # noqa: S603 - fixed argv, no shell
        [executable, *args], capture_output=True, text=True, timeout=timeout, cwd=REPO, check=False
    )


@pytest.fixture(scope="module")
def bag() -> dict:  # type: ignore[type-arg]
    if shutil.which("docker") is None:
        pytest.skip("docker is not on PATH")
    if docker("image", "inspect", IMAGE).returncode != 0:
        pytest.skip(
            f"image {IMAGE} not built (docker compose --profile airflow build airflow-init)"
        )
    result = docker(
        "run", "--rm",
        "-v", f"{REPO / 'airflow' / 'dags'}:/opt/airflow/dags:ro",
        "-e", "AIRFLOW__CORE__LOAD_EXAMPLES=false",
        "-e", "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow-test.db",
        "-e", "AIRFLOW__SCARF_ANALYTICS__SCARF_ANALYTICS=false",
        "--entrypoint", "python", IMAGE, "-c", INSPECT,
    )  # fmt: skip
    marker = [ln for ln in result.stdout.splitlines() if ln.startswith("RESULT=")]
    assert marker, (
        f"no result from the container:\n{result.stdout[-1500:]}\n{result.stderr[-1500:]}"
    )
    return json.loads(marker[-1].removeprefix("RESULT="))  # type: ignore[no-any-return]


def test_airflow_imports_every_dag_without_errors(bag: dict) -> None:  # type: ignore[type-arg]
    assert bag["errors"] == {}
    assert set(bag["dags"]) == {
        "raw_to_warehouse_daily",
        "dbt_transform_and_test",
        "cross_cloud_reconciliation",
        "historical_backfill",
    }


def test_every_dag_has_the_concurrency_and_timeout_policy(bag: dict) -> None:  # type: ignore[type-arg]
    for dag_id, dag in bag["dags"].items():
        assert dag["catchup"] is False, dag_id
        assert dag["max_active_runs"] == 1 and dag["max_active_tasks"] == 2, dag_id
        assert dag["dagrun_timeout_s"] and dag["dagrun_timeout_s"] <= 4 * 3600, dag_id


def test_every_task_has_bounded_retries_timeouts_and_a_failure_callback(bag: dict) -> None:  # type: ignore[type-arg]
    for dag_id, dag in bag["dags"].items():
        for task_id, task in dag["tasks"].items():
            where = f"{dag_id}.{task_id}"
            assert 0 <= task["retries"] <= 2, where
            assert task["timeout_s"] and task["timeout_s"] <= 3600, where
            assert task["has_failure_callback"], where
            if task["retries"]:
                assert task["backoff"] and task["max_retry_delay_s"] <= 600, where


def test_warehouse_tasks_share_the_single_writer_pool(bag: dict) -> None:  # type: ignore[type-arg]
    pooled = {
        (dag_id, task_id)
        for dag_id, dag in bag["dags"].items()
        for task_id, task in dag["tasks"].items()
        if task["pool"] == "duckdb_warehouse"
    }
    assert pooled == {
        ("raw_to_warehouse_daily", "load_new_batches"),
        ("dbt_transform_and_test", "dbt_build"),
        ("dbt_transform_and_test", "quality_checks"),
        ("dbt_transform_and_test", "build_dashboard"),
        ("cross_cloud_reconciliation", "export_metrics"),
        ("historical_backfill", "load_range"),
        ("historical_backfill", "dbt_build"),
        ("historical_backfill", "quality_checks"),
    }


def test_load_publishes_the_dataset_that_schedules_the_dbt_dag(bag: dict) -> None:  # type: ignore[type-arg]
    uri = "ecomm://warehouse/raw_events"
    load_outlets = bag["dags"]["raw_to_warehouse_daily"]["tasks"]["load_new_batches"]["outlets"]
    assert load_outlets == [uri]
    assert (
        "Dataset" in bag["dags"]["dbt_transform_and_test"]["schedule"]
        or uri in bag["dags"]["dbt_transform_and_test"]["schedule"]
    )


def test_task_order_inside_each_dag(bag: dict) -> None:  # type: ignore[type-arg]
    dags = bag["dags"]
    assert dags["raw_to_warehouse_daily"]["edges"] == ["check_lake_ready>load_new_batches"]
    assert dags["dbt_transform_and_test"]["edges"] == [
        "dbt_build>quality_checks",
        "quality_checks>build_dashboard",
    ]
    assert dags["historical_backfill"]["edges"] == [
        "dbt_build>quality_checks",
        "load_range>dbt_build",
        "validate_range>load_range",
    ]
    assert dags["cross_cloud_reconciliation"]["edges"] == []  # independent checks


def test_backfill_takes_a_validated_date_range(bag: dict) -> None:  # type: ignore[type-arg]
    assert bag["dags"]["historical_backfill"]["params"] == ["end_date", "only_new", "start_date"]
    assert bag["dags"]["historical_backfill"]["schedule"] in ("None", "Never", "NullTimetable")
