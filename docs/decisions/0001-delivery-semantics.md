# ADR 0001: Delivery semantics are at-least-once plus idempotent writes

- Status: accepted
- Date: 2026-11-10

## Context

Events travel Kafka -> Spark -> two object stores (S3, GCS) -> BigQuery.
Kafka idempotent producers and Spark checkpoints reduce duplicates inside one
system, but no transaction spans Kafka, two clouds and a warehouse.

## Decision

Design for **at-least-once delivery with idempotent sinks**:

- `event_id` is the stable deduplication key everywhere.
- Raw object keys are deterministic (`run_id` + partition), so a retry
  overwrites the same object instead of creating a second one.
- Each batch has a manifest; a batch is `COMPLETE` only when every required
  destination agrees with it.
- Warehouse loads go to a staging table and `MERGE` on stable keys.
- Replaying a batch must leave business aggregates unchanged (tested).

## Consequences

- Documentation and the README never claim exactly-once delivery.
- Version 1 is dual-write replication with reconciliation, not automatic
  failover.
