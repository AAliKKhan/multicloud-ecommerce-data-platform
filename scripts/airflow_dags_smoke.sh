#!/usr/bin/env bash
# Run every DAG once, end to end, with Airflow's own `dags test` (no scheduler, SQLite metadata).
#
# Runs INSIDE the ecomm-airflow image with the repo's dags/src/dbt and a data directory mounted
# (see .github/workflows/ci.yml). It may run as an arbitrary UID (the CI runner's), for which the
# image has no passwd entry; the official entrypoint normally adds one, but this script is started
# with --entrypoint bash, so it supplies the user name itself.
#
# Output contract: prints "DAG_FAILED <dag>" for every DAG whose run did not succeed and, only when
# ALL four succeeded, the final line "ALL_DAGS_OK". Callers must require that line: a script that
# died early prints neither marker, and absence of a failure message is not success.
set -uo pipefail

if ! whoami > /dev/null 2>&1; then
  export USER="${USER:-airflow}" LOGNAME="${LOGNAME:-airflow}"
fi
export HOME="${HOME:-/tmp}"
[ -w "${HOME}" ] || export HOME=/tmp

if ! airflow db migrate; then
  echo "SMOKE_SETUP_FAILED: airflow db migrate"
  exit 2
fi

failed=0
for dag in raw_to_warehouse_daily dbt_transform_and_test cross_cloud_reconciliation historical_backfill; do
  echo "=== DAG ${dag}"
  if ! airflow dags test "${dag}" 2026-11-10; then
    echo "DAG_FAILED ${dag}"
    failed=1
  fi
done

if [ "${failed}" -ne 0 ]; then
  exit 1
fi
echo "ALL_DAGS_OK"
