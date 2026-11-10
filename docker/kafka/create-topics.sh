#!/usr/bin/env bash
# Creates the pipeline topics. Idempotent: re-running changes nothing.
#
# Topic plan (see docs/02-ARCHITECTURE-AND-DATA-FLOW.md and ADR 0005):
#   <prefix>.orders      key = order_id
#   <prefix>.payments    key = payment_id
#   <prefix>.inventory   key = product_id:location_id
#   <prefix>.customers   key = customer_id
#   <prefix>.dlq         unified dead-letter topic
#
# Partitions: 3 for the business topics so that per-key ordering and parallel
# consumers can both be demonstrated; 1 for the dead-letter topic. Replication
# factor is 1 because the local stack has a single broker, which provides NO
# broker fault tolerance.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:19092}"
PREFIX="${KAFKA_TOPIC_PREFIX:-commerce}"
KAFKA_BIN=/opt/kafka/bin
# 30 days. The producer stamps messages with their simulated arrival time, which for a
# replayed historical window is in the past; retention is measured against message
# timestamps, so a short retention could delete the segment while it is still needed.
RETENTION_MS=2592000000

create() {
  local name="$1" partitions="$2"
  "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${PREFIX}.${name}" \
    --partitions "${partitions}" \
    --replication-factor 1 \
    --config retention.ms="${RETENTION_MS}" \
    --config min.insync.replicas=1
}

create orders 3
create payments 3
create inventory 3
create customers 3
create dlq 1

echo "Topics now present:"
"${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list | grep "^${PREFIX}\." || true
