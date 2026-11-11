#!/bin/sh
# Refuse to start MinIO without its generated root credentials (and say how to create them),
# instead of falling back to MinIO's well-known default login.
# Usage (entrypoint wrapper):  require-secrets.sh NAME[,NAME...] <command> [args...]
set -eu

names="$1"
shift
missing=""
for name in $(echo "$names" | tr ',' ' '); do
  eval "value=\${$name:-}"
  [ -z "$value" ] && missing="$missing $name"
done

if [ -n "$missing" ]; then
  {
    echo "ERROR: missing:$missing"
    echo "Create random, git-ignored secrets with:"
    echo "    python docker/minio/generate_env.py"
    echo "then start again: docker compose --profile minio up -d"
  } >&2
  exit 1
fi

exec "$@"
