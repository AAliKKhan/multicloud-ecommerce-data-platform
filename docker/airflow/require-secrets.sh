#!/usr/bin/env bash
# Refuse to start an Airflow service without the generated secrets, and say how to create them.
# Usage (entrypoint wrapper):  require-secrets.sh <command> [args...]
set -euo pipefail

missing=()
for name in AIRFLOW__DATABASE__SQL_ALCHEMY_CONN AIRFLOW__CORE__FERNET_KEY \
            AIRFLOW__WEBSERVER__SECRET_KEY POSTGRES_PASSWORD; do
  if [ -z "${!name:-}" ]; then
    missing+=("$name")
  fi
done

if [ "${#missing[@]}" -gt 0 ]; then
  {
    echo "ERROR: missing required settings: ${missing[*]}"
    echo "Create them (random values, git-ignored) with:"
    echo "    python docker/airflow/generate_env.py"
    echo "then start again: docker compose --profile airflow up -d"
  } >&2
  exit 1
fi

exec "$@"
