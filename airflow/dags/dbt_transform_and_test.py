"""Build the dbt models and run every dbt test, then the data-quality suite.

Triggered automatically when ``raw_to_warehouse_daily`` publishes new raw events (data-aware
scheduling), and can be triggered by hand at any time.

Tasks
-----
1. ``dbt_build``: ``ecomm-dbt``. Copies the dbt project and the product catalogue that belongs to the
   stream into ``<data dir>/dbt-runs/<run_id>/`` and runs ``dbt build`` there. A failed test fails the
   task; the logs and ``target/run_results.json`` stay in that directory. Warnings (for example
   orphan rows caused by quarantined events) do not fail the run.
2. ``quality_checks``: ``ecomm-quality``. Reconciliation, freshness and mart checks. Results are
   stored in ``raw.quality_results``; any FAIL fails the task, warnings do not.
3. ``build_dashboard``: renders the static HTML dashboard from the marts.

All three take the one-slot ``duckdb_warehouse`` pool (DuckDB has a single writer).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from ecomm_common import (
    DBT_PROJECT_DIR,
    DEFAULT_ARGS,
    RAW_EVENTS_DATASET,
    RUN_ENV,
    WAREHOUSE_POOL,
    cli,
    notify_failure,
)

with DAG(
    dag_id="dbt_transform_and_test",
    description="dbt build (models + tests), data-quality checks and the dashboard.",
    doc_md=__doc__,
    schedule=[RAW_EVENTS_DATASET],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    dagrun_timeout=timedelta(hours=2),
    default_args=DEFAULT_ARGS,
    on_failure_callback=notify_failure,
    tags=["ecomm", "dbt", "quality"],
) as dag:
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=cli("dbt", f"--project-dir '{DBT_PROJECT_DIR}'", '--run-id "$ECOMM_RUN_ID"'),
        env=RUN_ENV,
        append_env=True,
        pool=WAREHOUSE_POOL,
        execution_timeout=timedelta(minutes=45),
    )

    quality_checks = BashOperator(
        task_id="quality_checks",
        bash_command=cli("quality"),
        pool=WAREHOUSE_POOL,
        execution_timeout=timedelta(minutes=15),
    )

    build_dashboard = BashOperator(
        task_id="build_dashboard",
        bash_command=cli("report"),
        pool=WAREHOUSE_POOL,
        execution_timeout=timedelta(minutes=10),
    )

    dbt_build >> quality_checks >> build_dashboard
