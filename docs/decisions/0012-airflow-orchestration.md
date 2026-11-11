# ADR 0012: Airflow orchestration

- Status: accepted
- Date: 2026-11-11

## Context

The batch side needs scheduling, retries, backfills and a clear failure story, on a machine with
about 5 GB for containers, using DuckDB as the local stand-in for BigQuery.

## Decision

- **Airflow 2.10.5, LocalExecutor, PostgreSQL metadata**, as separate webserver and scheduler
  containers (about 2.5 GB together). No Celery/Kubernetes executor, no triggerer (nothing is
  deferrable). Airflow 3 was not used: the 2.x line is the stable, widely documented one for this scope.
- **DAGs stay thin; the work is in tested CLIs.** Each task is one shell command running a CLI that is
  already unit- and integration-tested (`ecomm-load`, `ecomm-dbt`, `ecomm-quality`, `ecomm-reconcile`,
  `ecomm-report`, `ecomm-metrics`). A DAG file contains orchestration only: order, retries, timeouts, pools.
  The tasks are therefore also runnable by hand with identical behaviour.
- **Isolated virtualenv for the project and dbt.** Airflow's constraints file pins packages that dbt's
  would conflict with. The image creates `/home/airflow/ecomm-venv` with exactly the `uv.lock` versions
  (a static test compares them), and BashOperator calls it. The source tree and dbt project are mounted
  read-only. `PythonOperator` was rejected because it would run inside Airflow's own interpreter.
- **Four DAGs as specified:** `raw_to_warehouse_daily`, `dbt_transform_and_test`,
  `cross_cloud_reconciliation`, `historical_backfill`.
- **Data-aware scheduling between load and dbt.** The load task publishes a dataset; the dbt DAG is
  scheduled on it. This avoids a clock guess ("run dbt at 03:00 and hope the load finished") and means a
  run with nothing new triggers nothing.
- **One-slot pool for DuckDB.** DuckDB allows a single writer process, and a second one fails on the file
  lock. Every task that opens the file takes the `duckdb_warehouse` pool (1 slot), so concurrent DAGs queue.
  This is a property of the stand-in, not of the design: BigQuery needs no pool.
- **"Nothing to do" is a skip, not a failure.** `check_lake_ready` exits 99, which `skip_on_exit_code`
  turns into SKIPPED for it and its dependants. A missing lake is a failure.
- **Run-bounded and idempotent.** Retries: 2, exponential backoff, 10-minute cap. `execution_timeout` per
  task, `dagrun_timeout` per run, `max_active_runs=1`, `max_active_tasks=2`. `catchup=False`: loads are
  keyed by committed batches, not by calendar slices, so replaying a missed day adds nothing.
- **Failure notification is a structured log line**, not email: no credentials belong in this repository.
  It carries DAG, task, run ID, try, log URL and a per-task hint.
- **Backfill input is untrusted.** Parameters are validated by JSON schema (format and pattern), by a
  `validate_range` task (raises `AirflowFailException`, which is not retried) and by the CLI argument
  parser, and reach the shell only as quoted environment variables.
- **Secrets are generated, never defaulted.** `docker/airflow/generate_env.py` creates random values in a
  git-ignored file; services refuse to start without them. The UI is on loopback and requires login.
- **DAGs start paused**, so bringing the stack up never runs anything unexpectedly.
- **Streaming is not an Airflow task.** The Spark job is a long-running or on-demand process with its own
  recovery (ADR 0011); making it a scheduled task would couple the stream's availability to the scheduler.

## Consequences

- Observed while testing (kept honest in docs/airflow.md): the retry policy recovered from a genuine
  failure (root-only files left by an older Spark run); `max_active_runs=1` queued concurrent triggers;
  triggering the load three times produced identical data; paused DAGs keep manual triggers queued, which
  looks like a hang until you unpause.
- Each Airflow CLI call takes several seconds on a small machine; the UI is the better tool interactively.
- Airflow metrics (DAG and task durations, retries) are not exported to Prometheus yet; that needs a StatsD
  exporter in the monitoring step.
