#!/bin/sh
# One-shot job: prepare the MinIO stand-in for the pipeline. Safe to run again.
#   1. create the private raw-zone bucket (versioned, like the Terraform bucket)
#   2. create the least-privilege policy from pipeline-policy.json (the same statements as the
#      Terraform pipeline role: list/get/put under five prefixes, no delete, no other bucket)
#   3. create the pipeline user and attach that policy to it
# Plain IAM user + policy on purpose: no service accounts or STS (see ADR 0015).
# Uses the root credentials; the pipeline itself only ever gets the pipeline user's key.
set -eu

for name in MINIO_ROOT_USER MINIO_ROOT_PASSWORD ECOMM_S3_BUCKET ECOMM_S3_ACCESS_KEY ECOMM_S3_SECRET_KEY; do
  eval "value=\${$name:-}"
  if [ -z "$value" ]; then
    echo "ERROR: $name is not set. Run: python docker/minio/generate_env.py" >&2
    exit 1
  fi
done

mc_cmd() { mc --config-dir /tmp/mc "$@"; }
POLICY=ecomm-pipeline

tries=0
until mc_cmd alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do
  tries=$((tries + 1))
  if [ "$tries" -ge 60 ]; then
    echo "ERROR: MinIO did not accept the root login after 60 tries" >&2
    exit 1
  fi
  sleep 2
done

mc_cmd mb --ignore-existing "local/$ECOMM_S3_BUCKET"
mc_cmd version enable "local/$ECOMM_S3_BUCKET"
mc_cmd anonymous set none "local/$ECOMM_S3_BUCKET"

sed "s/__BUCKET__/$ECOMM_S3_BUCKET/g" /policy.json > /tmp/policy.json
mc_cmd admin policy create local "$POLICY" /tmp/policy.json

mc_cmd admin user add local "$ECOMM_S3_ACCESS_KEY" "$ECOMM_S3_SECRET_KEY"
if ! out=$(mc_cmd admin policy attach local "$POLICY" --user "$ECOMM_S3_ACCESS_KEY" 2>&1); then
  # Re-running attaches an already attached policy: that is fine, anything else is not.
  echo "$out" | grep -qi "already" || { echo "$out" >&2; exit 1; }
fi

echo "MinIO ready: bucket $ECOMM_S3_BUCKET, pipeline user $ECOMM_S3_ACCESS_KEY (policy $POLICY)"
