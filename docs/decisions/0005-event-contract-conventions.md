# ADR 0005: Event contract conventions

- Status: accepted
- Date: 2026-11-10

## Context

The specification is inconsistent about the Kafka message key: section 2.3
says to use `event_id`, while the topic plan in 2.4 keys by entity ID
(`order_id`, `payment_id`, ...).

## Decision

- **Message key = entity ID** per the topic plan (`order_id`, `payment_id`,
  `product_id:location_id`, `customer_id`). This keeps all events of one
  entity ordered within a partition, which order/payment state logic needs.
- `event_id` stays the **deduplication key**. It lives in the envelope and is
  also sent as a Kafka header named `event_id`.
- Unknown envelope/payload fields are **ignored** (forward compatible).
  Missing or invalid required fields are violations. A breaking change bumps
  `schema_version`; unsupported versions are quarantined, not guessed at.
- Timestamps are ISO-8601 strings with an explicit zone, normalised to UTC
  (`...Z`, millisecond precision). Numeric timestamps are rejected.
- Money is a decimal string with two places (`"19.99"`), never a float.
- One `parse_event` function validates at the producer and consumer
  boundary. Violations carry a stable code (for quarantine records and metric
  labels) and never echo input values.
- Products are a reference dataset (dbt seed / catalog file), not a Kafka
  topic, because the topic plan has none.
- Payloads contain no names, e-mails, phone numbers, addresses or card data.
