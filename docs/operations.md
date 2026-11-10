# Operations guide (local mode)

Everything here runs without Docker or cloud credentials. Local substitutes:
two directories stand in for S3 and GCS, DuckDB stands in for BigQuery.

## Run the whole batch pipeline

```bash
uv sync --all-extras
uv run ecomm-demo --out data/demo --orders 300 --days 3
```

This generates events (with duplicates, late, out-of-order and malformed ones),
classifies them, lands deterministic raw batches in `s3-local` and `gcs-local`,
checks cross-"cloud" parity and loads DuckDB (`data/demo/warehouse.duckdb`).
It prints a summary and saves `data/demo/demo_summary.json`.

Then build the models:

```bash
# Windows (cmd):  set DBT_DUCKDB_PATH=data\demo\warehouse.duckdb
export DBT_DUCKDB_PATH=data/demo/warehouse.duckdb
cp data/demo/products.csv dbt/seeds/products.csv    # seed must match the generated catalogue
uv run dbt build --project-dir dbt --profiles-dir dbt --target duckdb
uv run dbt docs generate --project-dir dbt --profiles-dir dbt --target duckdb
```

Expected: no errors. Warnings on relationship tests are real data-quality
findings caused by quarantined parent events (see the data dictionary).

## Inspect results

- Quarantined messages: `data/demo/lake/s3-local/quarantine/source=commerce/...jsonl`
  (error code, detail, original payload, topic, partition, offset).
- Batch status ledger: `data/demo/lake/control/ledger/...` (`COMPLETE` / `PARTIAL` / `FAILED`).
- Load audit: table `raw.load_audit` (one row per attempt).
- Backfill reports: `data/demo/lake/control/reports/backfill/<run_id>.json`.

## Runbooks

**A cloud destination failed (batch `PARTIAL`).** The healthy destination is
intact and the failed one has no manifest, so it is *not committed*. Fix the
cause (credentials, quota, network) and call `commit_batch` again with the same
batch: committed destinations are skipped, the failed one is rewritten with the
same deterministic keys. Then run `reconcile_all`; commit only counts when
both destinations agree.

**Warehouse load failed.** Raw files are untouched. Read the error in
`raw.load_audit`, fix it, and re-run `WarehouseLoader.load_batch` or
`backfill(start, end, run_id)`. Replays are safe: already-present events are
counted as duplicates and nothing is inserted twice. Use `skip_succeeded=True`
to skip batches whose latest attempt succeeded.

**Duplicates suspected in the warehouse.** `raw_events` is unique on
`event_id` (a dbt test enforces it). Replay the batch and confirm
`rows_inserted = 0` in the audit; totals must not change. Add a regression test.

**Bad schema deployed.** Violations carry codes such as
`UNSUPPORTED_SCHEMA_VERSION` or `INVALID_PAYLOAD` in quarantine. Raw payloads
are preserved; add backward-compatible parsing or a new versioned schema and
replay the quarantined messages.

## What is verified and what is not

| Area | Status |
|---|---|
| Contracts, generator, ingest logic, manifests, retries, parity | Unit-tested |
| S3 adapter | Tested against `moto` (not real AWS) |
| GCS adapter | Tested against a fake client (not real GCS) |
| Warehouse loader and dbt models | Tested on DuckDB, with an independent metric cross-check |
| BigQuery client and BigQuery SQL | **Unverified on real BigQuery.** DDL rendering and call flow are tested with a fake; the compiled SQL is parsed as BigQuery SQL offline (syntax only) |
| Kafka, Spark, Airflow, Prometheus/Grafana, Metabase | Not built yet (Docker phase) |
| Terraform | Not built yet; nothing has been applied anywhere |

## Cleaning up

Everything under `data/` is local and git-ignored: delete the folder to reset.
No cloud resources exist, so there is nothing to tear down.
