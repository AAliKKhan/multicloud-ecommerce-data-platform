#!/bin/sh
# Refuse to start a BI service without its generated secrets (and say how to create them).
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
    echo "    python docker/bi/generate_env.py"
    echo "then start again: docker compose --profile bi up -d"
  } >&2
  exit 1
fi

exec "$@"
