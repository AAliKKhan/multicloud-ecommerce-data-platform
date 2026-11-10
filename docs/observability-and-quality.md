# Data quality, observability and the dashboard (local mode)

Everything here runs without Docker or cloud accounts. **Prometheus and Grafana
have not been run**: their configuration files are written and checked by tests,
but they need the Docker phase to be started.

## Commands

```bash
uv run ecomm-demo --out data/demo                  # generate, ingest, land, load
cp data/demo/products.csv dbt/seeds/products.csv
DBT_DUCKDB_PATH=data/demo/warehouse.duckdb uv run dbt build --project-dir dbt --profiles-dir dbt --target duckdb

uv run ecomm-quality    --demo-dir data/demo       # run + persist quality checks (exit 1 on FAIL)
uv run ecomm-quarantine --demo-dir data/demo       # inspect rejected events
uv run ecomm-metrics    --demo-dir data/demo --textfile data/demo/metrics.prom
uv run ecomm-metrics    --demo-dir data/demo --serve 9108    # live /metrics (loopback only)
uv run ecomm-report     --demo-dir data/demo       # writes data/demo/dashboard.html
```

## Data-quality checks (`ecomm-quality`)

Results are stored in the warehouse table `raw.quality_results` and as JSON in
the control store, so they can be queried and surfaced. A check that cannot run
becomes a `FAIL` with the reason (never a crash). Optional inputs that are absent
give `SKIP`, never a failure. Warnings are expected findings; failures are
broken guarantees.

| Area | Checks |
|---|---|
| Raw ledger | required fields not null, `event_id` unique, plausible event times |
| Marts | amounts and quantities in bounds, accepted currencies, items reference orders and products, payments reconcile to orders, daily sales reconcile to orders |
| Referential (warning) | payments without an order, orders without a customer (caused by quarantined parents) |
| Freshness | last successful load within the SLA (fail), newest event within the SLA (warn) |
| Source to target | every committed batch loaded, `rows_read` equals the manifest count, warehouse rows equal distinct events in loaded batches |
| Cross-cloud | S3 and GCS batches match (count and logical checksum) |
| Pipeline | every ledger batch is `COMPLETE`; quarantine rate (warn at 5%, fail at 25%) |

Note: the demo's latest event is older than the 24h SLA because the synthetic
window is in the past, so `freshness.latest_event` warns by design.

## Quarantine workflow (`ecomm-quarantine`)

Every rejected message keeps its error code, detail, original payload and
Kafka topic/partition/offset. The tool summarises by code and topic, filters
(`--code INVALID_JSON`), prints truncated samples and gives an operator hint per
code. After a fix, replay from the preserved payload (see the runbook for a bad
schema deployment in [operations.md](operations.md)).

## Metrics (`ecomm-metrics`)

Pipeline health (`ecomm_*`) is kept apart from business totals
(`ecomm_business_*`). Labels are only `status`, `event_type` and `section`, never
IDs. If one section fails to collect, the others still report and
`ecomm_scrape_errors{section}` is set.

Key metrics: `ecomm_batches{status}`, `ecomm_events_valid|duplicates|quarantined|late`,
`ecomm_quarantine_rate`, `ecomm_seconds_since_last_successful_load`,
`ecomm_load_attempts{status}`, `ecomm_parity_batches_mismatched`,
`ecomm_quality_checks{status}`.

Not yet exposed (they come from services that do not exist until the Docker
phase): Kafka consumer lag, Spark input rows and batch duration, Airflow DAG and
task metrics, and per-destination object write latency.

## Alerts and dashboard

`monitoring/prometheus/alerts.yml` defines: `PipelineStale`, `BatchesNotComplete`,
`WarehouseLoadFailures`, `HighQuarantineRate`, `CloudParityMismatch`,
`DataQualityChecksFailing`, `MetricsCollectionBroken`.
`monitoring/grafana/dashboards/pipeline_health.json` shows health panels first and
business totals in a separate row.

Verified by tests (not by a running Prometheus): every metric referenced by the
alerts and the dashboard exists in the exporter, no alert fires on a healthy run,
and in a deliberately broken run (partial batch, missing manifest, failed load,
quarantine flood, failing quality run, corrupt ledger) the matching alerts fire
under a small evaluator of the expression shapes used. A real Prometheus has not
evaluated them.

## Business dashboard (`ecomm-report`)

A single self-contained HTML file (inline SVG, no JavaScript, no external
requests) built from the dbt marts using the documented metric definitions. It is
a **local stand-in for a BI tool** such as Metabase, not a Metabase deployment.
Every page is labelled as synthetic data.
