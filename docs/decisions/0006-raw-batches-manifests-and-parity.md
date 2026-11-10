# ADR 0006: Raw batches, manifest commit protocol and parity checks

- Status: accepted
- Date: 2026-11-10

## Decision

- **Object key:**
  `raw/<source>/<event_type>/event_date=YYYY-MM-DD/event_hour=HH/part-<batch_id>.jsonl`.
  `<source>` is the Kafka topic prefix (`commerce`). The spec's
  `part-<run_id>-<partition>` is realised as `part-<batch_id>` where
  `batch_id = <run_id>-b<NNNNN>`, so one run can produce many batches without
  key collisions.
- **Deterministic content:** records are sorted by `(event_time, event_id)` and
  serialised with sorted keys, so the same logical batch yields byte-identical
  objects. Retries overwrite with identical bytes; clouds can be compared by
  SHA-256.
- **Record shape:** `{"ingest": {run_id, batch_id, source_topic,
  kafka_partition, kafka_offset, ingested_at, is_late}, "event": <original
  envelope, untouched>}`. Lineage sits beside the immutable event.
- **Commit protocol:** per destination, write data objects, read each back and
  verify SHA-256, then write `manifests/source=<s>/batch_id=<b>/manifest.json`
  **last**. A manifest is the commit marker; consumers (the warehouse loader)
  only read objects listed in a manifest.
- **Batch status:** `COMPLETE` when every required destination is committed,
  `PARTIAL` when some are, `FAILED` otherwise. Retrying the same batch skips
  committed destinations and rewrites only failed ones. The latest status is
  written to a control-plane ledger (`ledger/...json`).
- **Two checksums:** per-object SHA-256 (bytes) and a `logical_checksum` over
  the sorted set of event IDs (layout independent). Parity reconciliation
  re-reads each cloud, verifies it against its own manifest, then compares
  clouds on event count and logical checksum.
- **Retry:** exponential backoff, bounded attempts and maximum delay.

## Consequences

- This is dual-write replication with reconciliation. It is not failover, and
  it is not a backup until retention, deletion protection and restore are
  tested (spec 6.4).
- S3 behaviour is verified against `moto`; GCS against a fake client. Neither
  is verified against a real cloud account yet.
