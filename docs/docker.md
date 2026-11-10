# Docker stack (local development)

This is a **development topology, not a highly available cluster**. Kafka is a
single broker in KRaft mode (no ZooKeeper), so there is no broker fault
tolerance and every replication factor is 1.

Currently in the stack: Kafka, a one-shot topic-creation job, and Kafka UI.
Spark, Airflow, MinIO, Prometheus, Grafana and Metabase are added in later steps.

## Requirements

- Docker Desktop with the WSL2 backend (Windows) or Docker Engine (Linux/macOS).
- On a machine with 8 GB RAM, cap the WSL2 VM, for example in `%USERPROFILE%\.wslconfig`:

  ```ini
  [wsl2]
  memory=5GB
  processors=3
  swap=2GB
  ```

  Then run `wsl --shutdown` and restart Docker Desktop. The current Kafka stack
  uses roughly 1.2 GB; the memory limits in `docker-compose.yml` keep it from
  growing.

## Commands

```bash
docker compose up -d kafka kafka-init kafka-ui   # start; kafka-init exits after creating topics
docker compose ps -a                              # kafka healthy, kafka-init Exited (0)
docker compose logs kafka-init                    # topic creation output
docker compose down                               # stop, keep data
docker compose down -v                            # stop and DELETE the Kafka data volume
```

| Service | Address | Notes |
|---|---|---|
| Kafka (from your machine) | `127.0.0.1:9092` | IPv4 loopback only |
| Kafka (from other containers) | `kafka:19092` | internal listener |
| Kafka UI | http://127.0.0.1:8080 | no authentication: loopback only, never expose it |

Ports are published on `127.0.0.1` only. Change them with `KAFKA_HOST_PORT` and
`KAFKA_UI_PORT` if they clash.

## Topics

Created by `docker/kafka/create-topics.sh` (idempotent, safe to re-run):

| Topic | Key | Partitions |
|---|---|---|
| `commerce.orders` | `order_id` | 3 |
| `commerce.payments` | `payment_id` | 3 |
| `commerce.inventory` | `product_id:location_id` | 3 |
| `commerce.customers` | `customer_id` | 3 |
| `commerce.dlq` | n/a (dead letters) | 1 |

Messages with the same key always go to the same partition, so all events of
one order (or payment, or stock location) stay in order. `event_id` is the
de-duplication key and is also sent as a message header. Retention is 7 days.
Topics are never auto-created, so a typo cannot silently create a new topic.

## Producing and inspecting

```bash
uv sync --extra kafka                                  # installs confluent-kafka
uv run ecomm-kafka health                              # broker reachable, topics present
uv run ecomm-kafka produce --orders 200 --seed 42 --rate 50
uv run ecomm-kafka consume --topic orders --max 5      # read back (never commits offsets)
uv run ecomm-kafka topics --create                     # create topics without the init job
```

`produce` sends everything the generator emits, **including deliberately
malformed, duplicate, late and out-of-order messages**, so the consumer-side
validation and quarantine are exercised. Use `--validate` to make the producer
refuse invalid messages instead.

Exit codes: `0` all delivered, `1` some message failed or the run was
interrupted, `2` bad arguments, `3` Kafka unreachable or client library missing.
Ctrl+C stops cleanly: queued messages are flushed and the report says
`interrupted`.

### Producer guarantees (and limits)

- Idempotent producer, `acks=all`, LZ4 compression, bounded delivery time
  (`message.timeout.ms`), so an outage becomes a **visible failure**, never an
  endless wait or silent loss.
- Failed messages get further bounded rounds with exponential backoff
  (default 3 attempts). Permanent errors (message too large, authorization) are
  not retried.
- Delivery is **at-least-once**: a message re-sent after a reported failure may
  already have been written once. Consumers de-duplicate on `event_id`.
- Structured JSON logs go to stderr (identifiers and counts only, never payloads).

## Tests

```bash
uv run pytest tests/unit/producer tests/unit/infra     # no broker needed
docker compose up -d kafka kafka-init
uv run pytest -m integration                           # real broker; skipped if none
```

Integration tests create a throw-away topic prefix per run and delete it
afterwards; they never touch `commerce.*`.

## Troubleshooting

- **`not ready ... Start it with: docker compose up -d kafka kafka-init`**: the
  broker is down or topics are missing.
- **Slow first connection**: the first run after a cold start logs `Coordinator
  load in progress`. It is transient and retried automatically.
- **Data gone after `down -v`**: `-v` deletes the volume by design.
- **Out of memory**: lower other containers' limits or raise the WSL2 cap.
