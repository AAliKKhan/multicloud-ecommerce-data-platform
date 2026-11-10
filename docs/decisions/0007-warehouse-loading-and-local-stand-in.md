# ADR 0007: Warehouse loading (ELT) and the DuckDB local stand-in

- Status: accepted
- Date: 2026-11-10

## Context

BigQuery cannot run locally and no GCP account is available. The load path
and dbt models still need to be built and verified.

## Decision

- **ELT with an event ledger.** The loader lands every valid event into one
  typed table, `raw_events` (explicit schema; payload kept as JSON text; event
  time, date, lineage and load metadata as typed columns). Typed business
  entities are produced by dbt staging models. This keeps the loader simple,
  makes schema evolution a dbt change, and keeps full replay history.
- **Idempotent load:** per committed batch, bulk-load into a per-batch staging
  table, then `MERGE ... WHEN NOT MATCHED THEN INSERT` on `event_id`, with a
  static `event_date` window in the join so BigQuery can prune partitions.
  Replaying a batch inserts nothing.
- **Reconciliation:** source checksum check, `rows_read == manifest.event_count`,
  post-merge presence check, and an audit row per attempt (`attempt`,
  counts, status, error). `latest_status` orders by `attempt`, not by wall
  clock.
- **Datasets:** raw tables live in `BIGQUERY_DATASET`; dbt writes to
  `<BIGQUERY_DATASET>_analytics`. Locally these are the DuckDB schemas `raw`
  and `analytics`.
- **DuckDB is a local stand-in, not BigQuery.** It runs the identical loader
  and dbt models on a laptop. BigQuery-specific behaviour (partitioning,
  clustering, cost, quotas, JSON function semantics) is covered only by DDL
  rendering tests and a fake client, and is **unverified** until a real
  project is available.
- Bulk load jobs (not streaming inserts) are used for BigQuery to avoid
  streaming costs.
