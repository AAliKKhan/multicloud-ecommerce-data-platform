# Monitoring: Prometheus and Grafana

Prometheus scrapes two things: the **pipeline exporter** (batches, ingest outcomes, loads, quality, parity,
Spark progress, business totals) and the **Kafka exporter** (broker and partition offsets). Grafana shows one
provisioned dashboard on top. Design decisions and limits: [ADR 0013](decisions/0013-monitoring-stack.md).

Everything runs locally, in Docker, on synthetic data. The exporter reads the same lake and DuckDB warehouse the
pipeline writes (DuckDB stands in for BigQuery; local folders stand in for S3 and GCS).

## Start it

```bash
python docker/monitoring/generate_env.py --print-password    # once: a random Grafana login, git-ignored
docker compose up -d kafka                                   # Kafka exporter needs the broker
docker compose --profile monitoring up -d --build
```

| Service | Address | Notes |
|---|---|---|
| Prometheus | http://127.0.0.1:9090 | no authentication: loopback only |
| Grafana | http://127.0.0.1:3000 | user `admin`, password from the file; anonymous access and sign-up are off |
| Exporter, Kafka exporter | internal only | no published ports; only Prometheus scrapes them |

By default the exporter reads `./data/spark` (the Spark job's output). Point it elsewhere with
`PIPELINE_DATA_DIR=./data/demo`. It needs about 0.8 GB for the four services together; on an 8 GB machine stop
Kafka UI first (`docker compose stop kafka-ui`).

Grafana refuses to start without the generated password and prints how to create it; it never falls back to
its well-known default login.

## What is measured

| Area | Metrics | Source |
|---|---|---|
| Batches and loads | `ecomm_batches{status}`, `ecomm_load_attempts{status}`, `ecomm_seconds_since_last_successful_load` | ledger, load audit |
| Ingest | `ecomm_events_valid`, `_duplicates`, `_quarantined`, `_late`, `ecomm_quarantine_rate` | per-batch statistics |
| Cross-cloud parity | `ecomm_parity_batches_checked`, `ecomm_parity_batches_mismatched` | manifests |
| Quality | `ecomm_quality_checks{status}` | latest persisted quality run |
| Spark | `ecomm_spark_next_offset{topic,partition}`, `ecomm_spark_last_batch_timestamp_seconds` | per-batch progress records |
| Kafka | `kafka_brokers`, `kafka_topic_partition_current_offset` | Kafka exporter |
| Streaming lag | derived: Kafka end offset minus `ecomm_spark_next_offset` | Prometheus query |
| Exporter health | `ecomm_exporter_ready{reason}`, `ecomm_exporter_snapshot_age_seconds`, `ecomm_exporter_refresh_duration_seconds` | exporter |
| Business | `ecomm_business_net_sales_usd`, `ecomm_business_qualifying_orders` (clearly separated) | marts |

Spark has no Kafka consumer group (it tracks its own offsets), so group-based lag from the Kafka exporter would
show nothing. Lag is derived from Spark's recorded progress instead.

## Alerts (12 rules, all unit-tested)

`PipelineStale`, `BatchesNotComplete`, `WarehouseLoadFailures`, `HighQuarantineRate`, `CloudParityMismatch`,
`DataQualityChecksFailing`, `MetricsCollectionBroken`, `ExporterCannotReadPipelineData`, `ExporterSnapshotStale`,
`ScrapeTargetDown`, `KafkaBrokerDown`, `SparkConsumerLagHigh`. No notification channel is configured (that would
need credentials); alerts are visible in Prometheus (`/alerts`) and in the dashboard.

## How it is verified

- `promtool check config` and `check rules` on the pinned Prometheus image.
- `promtool test rules` runs [`alerts_test.yml`](../monitoring/prometheus/alerts_test.yml) through the **real
  Prometheus rule engine**: each alert is shown to fire only after its `for:` duration in a broken scenario, and a
  healthy scenario asserts none fire. A test requires every alert to appear in both kinds of scenario.
- With the stack running: every target is up, all rules load healthy and none fires on a healthy pipeline, the
  numbers match reality (batches, parity, brokers), Grafana rejects anonymous and default logins, the datasource
  and dashboard are provisioned read-only, and **every dashboard query runs through Grafana** without error and
  returns data (panels that need Spark progress may be empty until the job has run).
- CI repeats all of this on every push (job `monitoring`).

## Design points worth knowing

- **Scrapes never wait for work.** Computing the metrics reads the warehouse and walks the lake. On this
  machine that took over two minutes (1,846 small files on a Windows bind mount), which made every scrape time out
  and the container unhealthy. The exporter now refreshes in a background thread and serves the last snapshot
  from memory (a scrape takes about 10 ms), reporting its age so staleness is visible and alertable.
- **The slow part was a local directory walk, not the network.** Profiling one refresh inside the exporter image,
  with each step timed separately, found `LocalObjectStore.list_objects` walking and `stat`-ing every file under
  the store root before filtering by prefix: 10 to 56 seconds per call on the Windows bind mount, while reading a
  single file took 0.03 s and a MinIO listing 0.02 to 1.5 s. It now walks only the subtree the prefix can match
  (`os.walk`, no per-file `stat`): listing `manifests/` went from 14 to 28 s to under 0.4 s, and one whole refresh
  from 188 to 237 s to about 15 s on a local lake of 1,772 files, and from 31 to 36 s to 5 to 14 s with the S3 side
  in MinIO. Listing `raw/` still takes 7 to 10 s here because it genuinely holds hundreds of small files on a slow
  filesystem; on Linux it is milliseconds. A unit test proves the subtree property without timing.
- **Light parity while serving.** The live exporter compares manifests and object presence (cheap). Byte-level
  re-hashing of every object stays in the daily reconciliation DAG and in the one-shot export, where a full pass
  is acceptable. The light check cannot see an object altered in place; the daily job can.
- **A locked or missing warehouse is a state, not an outage.** If a writer holds DuckDB's lock, the exporter
  answers `ecomm_exporter_ready 0` with the reason instead of failing the scrape; the previous snapshot is
  served until the next successful refresh.

## Against a MinIO raw zone

With `ECOMM_S3_BACKEND=minio` the exporter container reads the S3 side of the raw zone from the bucket as the
least-privilege pipeline user (it only ever reads; the root credentials never reach it). Verified live
(`tests/integration/test_exporter_minio.py`, 5 checks, also in CI as job `containers-minio`):

- `ecomm_events_committed` is summed from the manifests read over S3 and equals what the pipeline committed; the
  cross-cloud parity check compares the bucket with the GCS copy and reports no mismatch; no scrape-error section
  is set; the bucket is byte-identical afterwards.
- A refresh against the bucket took 6 s for 2 batches here (measured by the exporter itself and asserted below 30 s).
- **MinIO outage.** With MinIO stopped the exporter keeps answering scrapes, serves its last snapshot and sets
  `ecomm_scrape_errors{section="lake"}` and `{section="parity"}`; when MinIO is back it recovers on its own with no
  restart. An alert on a growing `ecomm_exporter_snapshot_age_seconds` or on those error sections is how an outage
  would be noticed; no new alert rule was added for it.
- Each refresh builds a new S3 client (about 4 to 6 s on this starved machine, cold). Reusing one would shave that
  off but was not changed: it is well inside the 60-second cadence.

## When something is wrong

| Symptom | First step |
|---|---|
| `ecomm_exporter_ready` is 0 | Read `reason`: `starting` (first refresh running), `IOException` (a writer holds the warehouse lock; it recovers), `FileNotFoundError`/`CatalogException` (no warehouse or marts yet). |
| Prometheus shows 11 rules instead of 12 | Prometheus does not hot-reload rules. `docker restart ecomm-prometheus`. |
| Kafka exporter target is down | The broker is unhealthy; `docker compose restart kafka` (its data is on a volume). |
| Grafana will not start | The generated password file is missing; run `python docker/monitoring/generate_env.py`. |
| A dashboard panel is empty | Spark panels stay empty until the Spark job has recorded a batch; others need the pipeline to have run. |

## Not covered

- Spark's own driver metrics (they exist only while a query runs; the one-shot container does not keep a scrape
  target), and Airflow task/DAG metrics (they need a StatsD exporter that is not in the stack).
- Alert delivery (email, chat), long-term storage, and authentication on Prometheus.
- Throughput or capacity claims: nothing here has been load-tested.
