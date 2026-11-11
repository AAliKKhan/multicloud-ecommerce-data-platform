#!/bin/sh
# Refuse to start Grafana without the generated admin password (and say how to create it),
# instead of silently using Grafana's well-known default login.
# Usage (entrypoint wrapper):  require-secret.sh <command> [args...]
set -eu

if [ -z "${GF_SECURITY_ADMIN_PASSWORD:-}" ]; then
  {
    echo "ERROR: GF_SECURITY_ADMIN_PASSWORD is not set."
    echo "Create a random one (git-ignored) with:"
    echo "    python docker/monitoring/generate_env.py"
    echo "then start again: docker compose --profile monitoring up -d"
  } >&2
  exit 1
fi

exec "$@"
