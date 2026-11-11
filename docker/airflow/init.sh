#!/usr/bin/env bash
# One-shot initialisation, safe to run on every start:
#   1. migrate the metadata database,
#   2. create the admin user once (password from AIRFLOW_ADMIN_PASSWORD),
#   3. create/refresh the single-slot warehouse pool.
set -euo pipefail

if [ -z "${AIRFLOW_ADMIN_PASSWORD:-}" ]; then
  echo "ERROR: AIRFLOW_ADMIN_PASSWORD is not set (run: python docker/airflow/generate_env.py)" >&2
  exit 1
fi

airflow db migrate

if airflow users list 2>/dev/null | grep -qw "admin"; then
  echo "admin user already exists"
else
  airflow users create \
    --username admin \
    --password "${AIRFLOW_ADMIN_PASSWORD}" \
    --firstname Data \
    --lastname Platform \
    --role Admin \
    --email admin@example.invalid
fi

# DuckDB allows one writer process: tasks that open the warehouse file share this one slot.
airflow pools set duckdb_warehouse 1 "Serialises tasks that open the DuckDB warehouse file"

echo "airflow initialised"
