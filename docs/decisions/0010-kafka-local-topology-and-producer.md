# ADR 0010: Local Kafka topology and producer behaviour

- Status: accepted
- Date: 2026-11-10

## Decision

- **Broker:** `apache/kafka` (official image, pinned), single node in **KRaft**
  mode, one container acting as broker and controller. Replication factor 1
  everywhere. This provides no fault tolerance and is documented as such.
- **Listeners:** `HOST` (`127.0.0.1:9092`, published on the IPv4 loopback only)
  for tools on the developer machine, `INTERNAL` (`kafka:19092`) for containers,
  `CONTROLLER` internal. Clients force IPv4 because `localhost` can resolve to
  `::1` first and stall for seconds.
- **Data on a named volume:** `KAFKA_LOG_DIRS=/var/lib/kafka/data`. The image
  defaults to `/tmp`; an early run lost all messages on container recreation, so
  persistence is now asserted by a static test and was verified by recreating the
  container and reading messages back.
- **Topics:** created by an idempotent one-shot job, never by auto-creation.
  Partitions: 3 for business topics, 1 for the DLQ. Topic list is asserted equal to
  the event contract.
- **UI:** Kafka UI (the maintained `kafbat` fork), authentication off, so loopback
  only.
- **Producer:** `confluent-kafka` (librdkafka), idempotent, `acks=all`, bounded
  `message.timeout.ms`. Message key = entity ID (ADR 0005), `event_id` as header
  only when the value actually contains one, so ground-truth labels never leak.
- **Retry policy:** librdkafka retries internally within the timeout; the
  publisher adds bounded rounds with exponential backoff. Local transient errors
  (timeout, transport) are retriable; permanent ones are not. Every message ends
  as delivered, failed or rejected, and the CLI exits non-zero otherwise.
- **Guarantee:** at-least-once (ADR 0001). Duplicates are expected and removed
  downstream by `event_id`.
- **Fault injection:** by default the producer sends malformed messages too, so
  the consumer-side quarantine is exercised. `--validate` gives strict
  producer-side validation.
- **Optional dependency:** the client library is the `kafka` extra, so the
  Docker-free pipeline keeps working without it.

## Consequences

- Kafka consumer-lag metrics and broker metrics are not exposed yet (a Kafka
  exporter joins the Prometheus scrape config in the monitoring step).
- Multi-broker behaviour, rebalancing under failure and exactly-once semantics
  are not demonstrated by this topology and are not claimed.
