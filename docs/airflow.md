# Airflow orchestration

Four DAGs coordinate the batch side of the platform. Streaming (Kafka to Spark to raw files) runs
**separately and continuously** (see "Streaming lifecycle" below); Airflow only moves what is already
committed. Design decisions and limits: [ADR 0012](decisions/0012-airflow-orchestration.md).

Local substitutes: the two raw destinations are directories standing in for S3 and GCS, and DuckDB
stands in for BigQuery. Airflow 2.10.5, `LocalExecutor`, PostgreSQL metadata database.

## The DAGs

| DAG | Schedule | What it does |
|---|---|---|
| `raw_to_warehouse_daily` | `@daily` | `check_lake_ready` (fails if the lake is missing; **skips** if nothing is committed yet) then `load_new_batches` (`ecomm-load --only-new`). Publishes the `ecomm://warehouse/raw_events` dataset. |
| `dbt_transform_and_test` | when that dataset is updated | `dbt_build` (models **and** tests, `ecomm-dbt`) then `quality_checks` (`ecomm-quality`) then `build_dashboard`. |
| `cross_cloud_reconciliation` | `@daily` | `reconcile_destinations` (compares both raw destinations batch by batch) and `export_metrics` (Prometheus text file). Independent tasks; the run fails if either fails. |
| `historical_backfill` | manual, with parameters | `validate_range` then `load_range` then `dbt_build` then `quality_checks`, for `start_date`, `end_date` (inclusive) and `only_new`. |

Every DAG: `catchup=False`, `max_active_runs=1`, `max_active_tasks=2`, a per-run `dagrun_timeout`, a
per-task `execution_timeout`, two retries with exponential backoff capped at 10 minutes, and a failure
callback. They are created **paused**: nothing runs until you unpause it.

## Start it

```bash
python docker/airflow/generate_env.py --print-password      # once: random secrets, git-ignored
docker compose stop kafka-ui                                # on an 8 GB machine, free memory first
docker compose --profile airflow up -d --build airflow-webserver airflow-scheduler
```

Open http://127.0.0.1:8081 (user `admin`, password from the command above). The ports are bound to
loopback only. The stack needs about 2.5 GB; the first build downloads about 2 GB and installs dbt.

Point it at the data directory the Spark job wrote (default `./data/spark`):

```bash
set AIRFLOW_DATA_DIR=./data/airflow-demo         # Windows;  export AIRFLOW_DATA_DIR=... elsewhere
```

Then, in the UI or the CLI (each Airflow CLI call takes several seconds on a small machine):

```bash
docker exec ecomm-airflow-scheduler airflow dags unpause raw_to_warehouse_daily
docker exec ecomm-airflow-scheduler airflow dags unpause dbt_transform_and_test
docker exec ecomm-airflow-scheduler airflow dags trigger raw_to_warehouse_daily
docker exec ecomm-airflow-scheduler airflow dags trigger historical_backfill \
    --conf '{"start_date": "2026-11-01", "end_date": "2026-11-03"}'
```

Unpausing a daily DAG starts its most recent scheduled run immediately; that is normal.

## Design points

- **Idempotent tasks.** Loads `MERGE` on `event_id`, so a retry or a manual re-trigger inserts nothing
  twice; dbt rebuilds tables from the raw ledger; reconciliation is read-only. Observed while testing:
  triggering the daily load three times produced three successful runs and one set of rows.
- **One warehouse writer.** DuckDB allows a single writer process, so every task that opens the file takes
  the one-slot `duckdb_warehouse` pool (created by `airflow-init`). Tasks wait their turn instead of failing
  on a lock. With BigQuery this pool would not be needed.
- **Data-aware scheduling.** The dbt DAG is triggered by the load DAG's dataset, not by a clock, so models
  are rebuilt exactly when new data arrived. A run with nothing new is skipped upstream and triggers nothing.
- **Run IDs everywhere.** `$ECOMM_RUN_ID` (the Airflow run ID, sanitised for file names) labels load reports,
  reconciliation reports and the dbt run directory.
- **Isolated dependencies.** Airflow's own packages are pinned by its constraints file, and dbt's would
  conflict. The project and dbt live in a separate virtualenv in the image (versions identical to
  `uv.lock`); DAG tasks call it through shell commands. The source tree is mounted read-only.
- **Input safety.** Backfill parameters are validated by JSON schema in the trigger form, by the
  `validate_range` task and by the loader's argument parser, and reach the shell only through quoted
  environment variables.
- **No secrets in source.** `generate_env.py` writes random values to `docker/airflow/.env` (git-ignored,
  owner-only permissions where supported). A service refuses to start without them and prints how to create
  them.

## When something fails

Each failure writes one structured log line (DAG, task, run ID, try number, log URL, error, and a hint)
through `on_failure_callback`; the hint per task is in `airflow/dags/ecomm_common.py`. No email or chat
integration is configured because that would need credentials; the log line is what an alerting tool would
scrape.

| Symptom | First step |
|---|---|
| `check_lake_ready` fails | The lake directory is missing. Run the Spark job (docs/spark.md) or fix `AIRFLOW_DATA_DIR`. |
| `check_lake_ready` is **skipped** | Nothing committed yet. Not an error. |
| `load_new_batches` / `load_range` fails | Raw files are untouched. Read `raw.load_audit`, fix the cause, retry. If the log says *Permission denied*, the lake files were written by an older version as root-only (`0600`); run `chmod -R a+rX` on the data directory once. |
| `dbt_build` fails | Open `<data dir>/dbt-runs/<run_id>/` (`dbt.log`, `target/run_results.json`). The latest 10 runs are kept. |
| `quality_checks` fails | A check FAILED; run `ecomm-quality` for the table. Warnings never fail the task. |
| `reconcile_destinations` fails | See `<data dir>/lake/control/reports/reconciliation/<run_id>.json`; re-commit the batch to the lagging destination (docs/operations.md). |
| Database lock error | Another process holds the DuckDB file (for example a local `dbt build`). Stop it; the pool only serialises Airflow tasks. |
| *Permission denied* creating a file under `lake/control/...` | Two different container users wrote the same data directory (for example the stack as UID 50000, then a one-off container as UID 1001); the second cannot write into folders the first created. Use one user per data directory (`AIRFLOW_UID`), or `chmod -R a+rwX` the directory once. CI uses a single user and a fresh directory. |
| `airflow db migrate` says the user has no username | A one-off `docker run --user <uid>:0 --entrypoint bash` skipped the official entrypoint, which normally creates the user entry. `scripts/airflow_dags_smoke.sh` sets `USER`/`LOGNAME` itself; do the same in your own commands. |

## Streaming lifecycle (separate from Airflow)

The Spark job is **not** an Airflow task. It runs on demand or continuously
(`docker compose --profile spark run --rm spark --trigger processing-time:10s`), commits batches with
manifests, and recovers from crashes by itself (docs/spark.md). If Airflow is down, streaming continues and
files keep landing; the next scheduled load picks up everything committed since, so loads wait and nothing
is lost. If Spark is down, Airflow simply finds nothing new and skips.

## What is and is not verified

- Verified here: all four DAGs import in a real Airflow 2.10.5; a complete workflow ran (daily load, then
  the dataset-triggered dbt run with tests, quality checks and dashboard); the retry policy recovered a real
  failure; `max_active_runs` queued concurrent runs; the pool serialised warehouse tasks.
- Verified in CI on every push: `airflow dags test` runs all four DAGs against a freshly generated lake as a
  non-root user and must print `ALL_DAGS_OK`; then the project's own quality suite and the reconciliation
  run on the warehouse the DAGs built. (An earlier version of that step passed while running nothing; it now
  keeps the exit code through the pipe and requires the explicit marker, with a test guarding both.)
- Not verified: BigQuery as the warehouse, real S3/GCS, multi-worker executors, and email/Slack alerting.
- Memory: Airflow plus Kafka plus Spark together do not fit in 5 GB; run Airflow with Kafka stopped (it does
  not need Kafka).
