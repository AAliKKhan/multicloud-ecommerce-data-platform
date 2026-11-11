"""Shared building blocks for the e-commerce DAGs.

This module deliberately imports only the standard library and Airflow itself: the project's own
code runs in an isolated virtualenv inside the image (so dbt's dependencies can never conflict
with Airflow's), and the DAGs reach it through shell commands built by :func:`cli`.

Conventions every DAG follows (spec section 5.10):

- **Bounded retries**: two retries, exponential backoff capped at 10 minutes.
- **Timeouts**: a per-task ``execution_timeout`` and a per-run ``dagrun_timeout``.
- **Concurrency limits**: ``max_active_runs=1`` and ``max_active_tasks=2`` per DAG, and every
  task that opens the single-writer DuckDB file takes the one-slot ``duckdb_warehouse`` pool.
- **Idempotent tasks**: each command can be re-run safely (loads ``MERGE`` on ``event_id``;
  dbt rebuilds tables from the raw ledger; reconciliation is read-only).
- **Run IDs**: the Airflow run ID is passed to every command as ``$ECOMM_RUN_ID``; reports and
  dbt artifacts are named after it.
- **No secrets**: nothing sensitive appears in DAG source. Credentials, if any, come from the
  environment of the Airflow containers (generated, git-ignored file).
"""

from __future__ import annotations

import json
import logging
import os
import shlex
from datetime import timedelta
from typing import Any

from airflow.datasets import Dataset

# --- where things live inside the containers (overridable from the environment) -------------------

DATA_DIR = os.environ.get("ECOMM_DATA_DIR", "/data/pipeline")
PYTHON = os.environ.get("ECOMM_PYTHON", "/home/airflow/ecomm-venv/bin/python")
SRC_DIR = os.environ.get("ECOMM_SRC", "/opt/ecomm/src")
DBT_PROJECT_DIR = os.environ.get("ECOMM_DBT_PROJECT", "/opt/ecomm/dbt")

#: One slot: DuckDB allows a single writer process, so tasks that open the warehouse take turns.
WAREHOUSE_POOL = "duckdb_warehouse"

#: Emitted by the daily load, consumed by the dbt DAG (data-aware scheduling).
RAW_EVENTS_DATASET = Dataset("ecomm://warehouse/raw_events")

#: Exit code that means "nothing to do" and marks the task (and its dependants) skipped.
SKIP_EXIT_CODE = 99

#: CLI name -> Python module (the project is on ``PYTHONPATH``, not pip-installed).
MODULES = {
    "load": "ecomm.warehouse.cli",
    "reconcile": "ecomm.lake.reconcile_cli",
    "dbt": "ecomm.orchestration.dbt_runner",
    "quality": "ecomm.quality.cli",
    "report": "ecomm.report.cli",
    "metrics": "ecomm.observability.cli",
}

#: Environment passed to every BashOperator (``{{ run_id }}`` is rendered by Airflow).
RUN_ENV = {"ECOMM_RUN_ID": "{{ run_id }}"}

BASH_STRICT = "set -euo pipefail\n"


def cli(tool: str, *args: str) -> str:
    """Shell command that runs one of the project's CLIs in the isolated virtualenv.

    ``args`` are inserted verbatim, so the caller quotes them (use ``'"$VAR"'`` for values taken
    from the environment). The data directory is always supplied.
    """
    module = MODULES[tool]
    parts = [
        f"PYTHONPATH={shlex.quote(SRC_DIR)}",
        shlex.quote(PYTHON),
        "-m",
        module,
        "--demo-dir",
        shlex.quote(DATA_DIR),
        *args,
    ]
    return BASH_STRICT + " ".join(parts)


# --- failure notification ------------------------------------------------------------------------------

#: What an operator should do first, per task. Shown in the failure log line.
FAILURE_HINTS = {
    "check_lake_ready": (
        "The raw lake directory is missing. Run the Spark job (docs/spark.md) so batches are "
        "committed, or point ECOMM_DATA_DIR at the right directory."
    ),
    "load_new_batches": (
        "Raw files are untouched. Read raw.load_audit for the failing batch, fix the cause and "
        "retry the task: loads are idempotent, so replays never double count."
    ),
    "dbt_build": (
        "dbt output and artifacts are kept in <data dir>/dbt-runs/<run_id>/ (dbt.log, "
        "target/run_results.json). Fix the failing model or test, then clear the task."
    ),
    "quality_checks": (
        "At least one data-quality check FAILED. Run `ecomm-quality` for the table of results; "
        "results are also stored in raw.quality_results. Warnings do not fail the task."
    ),
    "build_dashboard": "The marts are missing or unreadable; check the dbt_build task first.",
    "reconcile_destinations": (
        "The two destinations disagree for at least one batch. See "
        "<data dir>/lake/control/reports/reconciliation/<run_id>.json; re-commit the batch to "
        "the lagging destination (docs/operations.md, 'A cloud destination failed')."
    ),
    "export_metrics": "The warehouse could not be read; check the load and dbt tasks.",
    "validate_range": "The date range is invalid. Re-trigger with start_date <= end_date.",
    "load_range": (
        "Raw files are untouched. Read raw.load_audit for the failing batch and retry: the "
        "backfill is idempotent."
    ),
}
DEFAULT_HINT = "Open the task log from log_url, fix the cause, then clear the task to retry."


def build_failure_record(context: dict[str, Any]) -> dict[str, Any]:
    """The structured content of a failure notification (pure, so it is unit-tested)."""
    ti = context.get("task_instance")
    run = context.get("dag_run")
    task_id = getattr(ti, "task_id", None) or getattr(context.get("task"), "task_id", None)
    exception = context.get("exception")
    return {
        "event": "airflow_failure",
        "dag_id": getattr(run, "dag_id", None) or getattr(ti, "dag_id", None),
        "task_id": task_id,
        "run_id": getattr(run, "run_id", None) or getattr(ti, "run_id", None),
        "try_number": getattr(ti, "try_number", None),
        "max_tries": getattr(ti, "max_tries", None),
        "log_url": getattr(ti, "log_url", None),
        "error": str(exception)[:500] if exception is not None else None,
        "hint": FAILURE_HINTS.get(str(task_id), DEFAULT_HINT),
    }


def notify_failure(context: dict[str, Any]) -> None:
    """``on_failure_callback``: one structured log line with the run ID and guidance.

    Email/Slack are intentionally not configured (no credentials in this repository); the log line
    is what an alerting tool would scrape, and the Prometheus alert rules cover the data side.
    """
    logging.getLogger("airflow.task").error(
        json.dumps(build_failure_record(context), sort_keys=True)
    )


# --- defaults -----------------------------------------------------------------------------------------------

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": notify_failure,
}
