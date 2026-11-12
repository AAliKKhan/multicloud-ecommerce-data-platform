"""Load every newly committed raw batch into the warehouse, once a day.

Streaming runs separately (the Spark job, see docs/spark.md); this DAG only moves what is already
committed. A batch is *committed* once its manifest exists in the raw lake, so half-written data is
never read.

Tasks
-----
1. ``check_lake_ready``: fails if the raw zone is missing or unreachable (a local directory, or a
   MinIO/S3 bucket when ``ECOMM_S3_BACKEND=minio``); **skips** the run (and everything after it)
   when there is simply nothing committed yet. An outage is a failure, never a skip.
2. ``load_new_batches``: ``ecomm-load --only-new``. Batches whose latest load succeeded are skipped;
   the rest are verified (SHA-256, row counts) and ``MERGE``d on ``event_id``. Re-running is safe:
   replays insert nothing, so revenue can never be double counted.

On success the run publishes the ``ecomm://warehouse/raw_events`` dataset, which triggers
``dbt_transform_and_test``. If Airflow is down the stream keeps landing files; the next run loads
them (loads wait, nothing is lost).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from ecomm_common import (
    DEFAULT_ARGS,
    RAW_EVENTS_DATASET,
    RUN_ENV,
    SKIP_EXIT_CODE,
    WAREHOUSE_POOL,
    cli,
    notify_failure,
)

# Exit 0: batches present. Exit 99 (SKIP_EXIT_CODE): nothing committed yet. Anything else: fail.
# The check goes through the same lake factory as every other tool, so it works for a local
# directory and for a MinIO bucket alike (ecomm.lake.ready_cli).
CHECK_LAKE = cli("lake_ready")

with DAG(
    dag_id="raw_to_warehouse_daily",
    description="Load newly committed raw batches into the warehouse (idempotent MERGE).",
    doc_md=__doc__,
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    dagrun_timeout=timedelta(hours=1),
    default_args=DEFAULT_ARGS,
    on_failure_callback=notify_failure,
    tags=["ecomm", "load", "daily"],
) as dag:
    check_lake_ready = BashOperator(
        task_id="check_lake_ready",
        bash_command=CHECK_LAKE,
        skip_on_exit_code=SKIP_EXIT_CODE,
        execution_timeout=timedelta(minutes=5),
        retries=0,  # a missing directory will not appear by itself in a few minutes
    )

    load_new_batches = BashOperator(
        task_id="load_new_batches",
        bash_command=cli("load", "--only-new", '--run-id "$ECOMM_RUN_ID"'),
        env=RUN_ENV,
        append_env=True,
        pool=WAREHOUSE_POOL,
        outlets=[RAW_EVENTS_DATASET],
        execution_timeout=timedelta(minutes=20),
    )

    check_lake_ready >> load_new_batches
