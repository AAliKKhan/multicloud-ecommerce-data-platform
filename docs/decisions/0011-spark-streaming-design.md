# ADR 0011: Spark Structured Streaming design

- Status: accepted
- Date: 2026-11-10

## Context

Spark must consume Kafka, validate, de-duplicate and land raw files with manifests,
restart without double counting, and never lose late or invalid events silently. It must
also run on a laptop with a ~5 GB container budget.

## Decision

- **Local mode, one container.** `local[2]` with a 1 GB driver in a 2 GB container; no
  master/worker pair (the documented single-node deployment). The image is built from
  Python 3.11 + Java 17 + pip PySpark 3.5.6 with the Kafka connector jars baked in
  (the stock Spark images ship a Python older than the project's 3.11 requirement).
- **Pipeline:** `readStream` Kafka -> UDF validation (the same `parse_event` used at the
  producer boundary) -> `applyInPandasWithState` de-duplication on `event_id` ->
  `foreachBatch` sink. Invalid messages never leave the stream: they are quarantined
  with code, detail, original payload and Kafka coordinates.
- **Late events are kept.** Lateness is `Kafka timestamp - event_time > threshold`, a flag
  on the record (ADR 0002). The producer sets the Kafka timestamp to the generator's
  *simulated arrival time*, otherwise replaying a historical window would make every
  message look days late.
- **De-duplication state is checkpointed and bounded.** State is the last-seen time per
  `event_id`, in the checkpoint, so duplicates seen before a restart are still caught.
  It expires through event-time timeouts driven by a watermark on the Kafka timestamp
  (lagging by the retention, default 24 h). A later duplicate is not caught by Spark but
  by the idempotent warehouse `MERGE` (ADR 0001).
- **Why event-time, not processing-time timeouts:** with processing-time timeouts Spark
  always schedules another batch, so an `availableNow` query never terminates (observed
  first-hand: >120 empty batches). Event-time timeouts do terminate.
- **No silent loss from the watermark.** Spark drops rows older than its watermark. The
  sink therefore checks that Kafka offsets inside each micro-batch are consecutive per
  partition and fails the batch (`OffsetGapError`) otherwise. Edge rows of a batch cannot
  be checked this way; the per-batch offset ranges are recorded so continuity across
  batches can be audited (the integration test does).
- **Idempotent sink (exactly-once *effects*, not exactly-once delivery).**
  - `batch_id = <run_id>-b<spark micro-batch number>`; Spark replays a failed batch with
    the same number and the same offsets.
  - Everything written is deterministic in the batch's data, including `ingested_at`
    (= newest Kafka timestamp in the batch), so a replay produces byte-identical objects
    and an identical manifest.
  - A batch that does not reach `COMPLETE` raises, so the checkpoint does not advance.
  - A manifest or quarantine file that already exists with *different* content under the
    same batch ID is refused (`BatchConflictError`) before anything is written. This is
    what happens if the checkpoint is deleted but `--run-id` is not changed.
- **Commit protocol runs on the driver** (collect, then the tested dual-write commit with
  read-back verification and manifests). That is simple and fully unit-tested, but it
  caps throughput at what one driver can hold per batch (`--max-offsets-per-trigger`).
  A production version would write partition files from the executors and commit
  manifests on the driver.
- **Destinations are local directories** standing in for S3 and GCS, using the demo layout,
  so the loader, quality suite and metrics work on Spark output unchanged. MinIO and the
  real adapters replace them in the storage step.
- **Metrics:** per-batch input/valid/duplicate/quarantined/late counts and offset ranges
  are written to the control store (so `ecomm-metrics` already reports them); Spark's own
  Prometheus endpoint is enabled for the monitoring step.

## Consequences

- The job is slow at this scale: Python UDFs and a pandas state function on two cores take
  roughly 20 to 50 seconds per 500-message batch after a ~60 s warm-up. Throughput is not
  claimed and has not been benchmarked.
- The late-event flag depends on producer timestamps; with real traffic Kafka's own
  timestamp is the arrival time.
- State and checkpoint compatibility: changing the timeout type or state schema requires
  a new checkpoint directory.
