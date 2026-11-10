# ADR 0002: Event time, ingestion time and late events

- Status: accepted
- Date: 2026-11-10

## Decision

- `event_time` (UTC, from the producer) is never overwritten. `ingested_at`
  and `processed_at` are added by the pipeline.
- Operational latency uses ingestion time; business aggregation uses event
  time.
- `LATE_EVENT_THRESHOLD_MINUTES` (default 60) is the streaming lateness
  bound. Events later than the bound are **not dropped**: they go to a
  late-event path and are folded in by batch backfill.
- Mutable entities (orders) use an explicit version/timestamp ordering rule
  with `event_id` as tie-breaker, never "arbitrary last row".
- Payments keep a full event ledger; current status is derived separately.

## Consequences

Backfill must be idempotent and accept a start and end date.
