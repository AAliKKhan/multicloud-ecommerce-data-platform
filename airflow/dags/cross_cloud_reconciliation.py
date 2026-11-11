"""Check that the two raw destinations (S3 and GCS stand-ins) hold the same data, once a day.

Both tasks are independent and the run fails if either fails.

Tasks
-----
- ``reconcile_destinations``: ``ecomm-reconcile``. Re-reads every batch in both destinations,
  verifies each object against its own manifest (SHA-256, record counts) and compares the
  destinations on event count and the checksum of the set of event IDs. Read-only. Exit code 1 and
  a per-batch problem list when they disagree; the report is saved to
  ``<data dir>/lake/control/reports/reconciliation/<run_id>.json``.
- ``export_metrics``: writes the Prometheus text file ``<data dir>/metrics.prom`` (pipeline health,
  parity, quarantine rate), so a scraper sees the same facts this DAG checks.

This is dual-write replication with reconciliation, **not** automatic failover: a mismatch is
reported, and repaired by re-committing the batch to the lagging destination (the commit is
idempotent). A Spark batch is only marked COMPLETE when both destinations hold it.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from ecomm_common import (
    DATA_DIR,
    DEFAULT_ARGS,
    RUN_ENV,
    WAREHOUSE_POOL,
    cli,
    notify_failure,
)

with DAG(
    dag_id="cross_cloud_reconciliation",
    description="Compare both raw destinations batch by batch and export pipeline metrics.",
    doc_md=__doc__,
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    dagrun_timeout=timedelta(hours=1),
    default_args=DEFAULT_ARGS,
    on_failure_callback=notify_failure,
    tags=["ecomm", "reconciliation", "daily"],
) as dag:
    reconcile_destinations = BashOperator(
        task_id="reconcile_destinations",
        bash_command=cli("reconcile", '--run-id "$ECOMM_RUN_ID"'),
        env=RUN_ENV,
        append_env=True,
        execution_timeout=timedelta(minutes=20),
        retries=1,  # a mismatch is not transient; one retry covers a file still being replaced
    )

    export_metrics = BashOperator(
        task_id="export_metrics",
        bash_command=cli("metrics", f"--textfile '{DATA_DIR}/metrics.prom'"),
        pool=WAREHOUSE_POOL,  # opens the DuckDB file (read-only)
        execution_timeout=timedelta(minutes=10),
    )
