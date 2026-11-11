#!/usr/bin/env bash
# Run every DAG once, end to end, with Airflow's own `dags test` (no scheduler, SQLite metadata).
# Meant to run INSIDE the ecomm-airflow image with the repo's dags/src/dbt and a data directory
# mounted (see .github/workflows/ci.yml). Prints DAG_FAILED <dag> for any DAG that fails and exits
# non-zero if any did, so a failing task cannot pass unnoticed.
set -uo pipefail

airflow db migrate > /dev/null 2>&1 || { echo "airflow db migrate failed"; exit 2; }

failed=0
for dag in raw_to_warehouse_daily dbt_transform_and_test cross_cloud_reconciliation historical_backfill; do
  echo "=== DAG ${dag}"
  if ! airflow dags test "${dag}" 2026-11-10; then
    echo "DAG_FAILED ${dag}"
    failed=1
  fi
done
exit "${failed}"
