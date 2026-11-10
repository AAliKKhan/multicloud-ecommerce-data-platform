# ADR 0003: Storage formats and object layout

- Status: accepted
- Date: 2026-11-10

## Decision

- **Raw (bronze):** append-only newline-delimited JSON, kept for replay and
  schema evolution.
- **Validated (silver):** Parquet.
- **Curated (gold):** typed BigQuery tables, partitioned by event date and
  clustered on common join keys.
- Raw object key:
  `raw/<source>/<event_type>/event_date=YYYY-MM-DD/event_hour=HH/part-<run_id>-<partition>.jsonl`
- Never partition by high-cardinality IDs.
- Kafka partition and offset, `run_id` and `batch_id` are recorded per record.
- Provider SDK code lives only in storage adapters, behind a small interface
  (`write_object`, `read_object`, `list_objects`, `object_exists`).
