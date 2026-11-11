# Spark streaming job

The job reads the four business topics from Kafka, validates every message, de-duplicates on
`event_id`, quarantines invalid messages and lands partitioned raw files with manifests in
two destinations. Design decisions and limits: [ADR 0011](decisions/0011-spark-streaming-design.md).

Local substitutes: the two destinations are directories (`s3-local`, `gcs-local`) standing
in for S3 and GCS, under `data/spark/lake/`. They use the same layout as the Docker-free demo,
so the loader, quality suite and metrics work on this output unchanged. The S3 side can instead
be a bucket on a real S3-compatible server (MinIO): see [Landing the S3 side in MinIO](#landing-the-s3-side-in-minio).

## Run it

```bash
docker compose up -d kafka kafka-init                         # broker + topics
uv run ecomm-kafka produce --orders 200 --days 3 --seed 42 --products-csv data/spark/products.csv

docker compose --profile spark build spark                    # first time only (large download)
# This stream spans 3 days of event time, so the retention must cover it (see below):
docker compose --profile spark run --rm spark --dedup-retention-hours 96   # drain, then exit
docker compose --profile spark run --rm spark --trigger processing-time:10s   # keep running
```

Useful options (arguments after the service name go to the job):
`--max-offsets-per-trigger N` (micro-batch size), `--run-id`, `--dedup-retention-hours`,
`--late-threshold-minutes`, `--starting-offsets earliest|latest`.

Output under `data/spark/`: `lake/s3-local`, `lake/gcs-local` (raw, manifests, quarantine),
`lake/control` (ledger, ingest statistics, per-batch progress with offset ranges) and
`checkpoints/ingest`. Spark's UI is on http://127.0.0.1:4040 while a query is running.

## Landing the S3 side in MinIO

With one setting the job writes the S3 copy of the raw zone to a MinIO bucket instead of a local folder,
through the same S3 adapter that will talk to AWS. The GCS copy, the control store (ledger, statistics,
progress) and the checkpoint stay local. MinIO setup: [minio.md](minio.md).

```powershell
docker compose up -d kafka kafka-init
docker compose --profile minio up -d --build                 # then once: docker compose --profile minio run --rm minio-init
docker compose --profile spark build spark                   # the image now contains boto3
$env:ECOMM_S3_BACKEND = "minio"
docker compose --profile spark run --rm spark --dedup-retention-hours 96
uv run ecomm-load --demo-dir data/spark                      # the host reads the same bucket (same variable)
```

What the container gets: the **pipeline user's** key (a least-privilege user whose policy matches the
Terraform pipeline role) and the in-network address `http://minio:9000`; never the MinIO root credentials.
The MinIO and Spark profiles are independent, so a plain local run needs neither the secrets nor MinIO.

- **Fail fast.** Before Spark starts, the job lists both destinations. With MinIO stopped it exits `1` in
  seconds with `destination s3-minio is not reachable` and writes nothing, instead of booting the JVM and failing
  on the first batch minutes later. `ECOMM_S3_BACKEND=minio` without its settings exits `2` with the fix.
- **Unreadable is not the same as empty.** Before writing a batch the sink reads each destination to make
  sure it will not overwrite different data. If that read *fails* (an outage, a timeout) the batch now fails with
  a retryable `BatchCommitError` and **nothing is written anywhere**; before, the raw storage error escaped.
  Found while wiring this up; covered by unit tests written first.
- **Verified end to end** (`tests/integration/test_spark_minio.py`, 13 checks, real Kafka, the real container and
  real MinIO): crash right after a commit, MinIO stopped (fails fast, writes nothing, the local GCS copy is
  untouched), restart (replays the crashed batch), rerun with nothing new (changes nothing). The result equals
  the pure-Python reference ingest (valid, duplicate, quarantined and late counts; no event written twice; every
  quarantine reason; late flags), the MinIO and GCS copies are byte-identical, Kafka offsets are covered without
  gaps, and a warehouse loaded from MinIO has no double counting and a replay adds nothing.
- **Not tested:** MinIO going away *while* a micro-batch is running (the timing is not reproducible enough for a
  reliable test; the unit tests cover the sink's behaviour on an unreadable or unwritable destination), and
  anything that needs real AWS (see [ADR 0015](decisions/0015-minio-local-s3.md)).

## Then load, model and report

```bash
uv run ecomm-load --demo-dir data/spark                        # idempotent load into DuckDB
copy data\spark\products.csv dbt\seeds\products.csv            # the seed must match the stream
set DBT_DUCKDB_PATH=data\spark\warehouse.duckdb
uv run dbt build --project-dir dbt --profiles-dir dbt --target duckdb
uv run ecomm-quality --demo-dir data/spark
uv run ecomm-report  --demo-dir data/spark                     # data/spark/dashboard.html
```

## Behaviour you can rely on (all tested)

- **Same answer as the reference ingest.** Valid, duplicate, quarantined and late counts equal
  those of the pure-Python ingest over the same messages (integration test).
- **Restart without double counting.** The integration test crashes the job right after a batch
  is committed and before Spark checkpoints it, restarts it, and then runs it again with
  nothing new to read. No event is written twice, and the objects and manifests are
  byte-identical to an uninterrupted run; a rerun with nothing new changes nothing.
- **Duplicates across a restart** are still recognised because the de-duplication state is in the
  checkpoint.
- **Late events are kept and flagged**, never dropped.
- **Nothing is lost silently.** A batch that cannot reach both destinations raises and is retried
  from the checkpoint. Before writing anything, every batch is checked against the offsets Spark
  itself planned for it (read from the checkpoint): if even one planned message did not reach the
  sink the batch fails with `OffsetGapError` and the messages stay in Kafka. See the next
  section for why this exists.
- **Deleting the checkpoint is refused unless you change the run id.** Batch numbers restart at
  0; a batch whose data differs from an already committed one fails with `BatchConflictError`
  instead of overwriting it. Use `--run-id` with a new value (or remove `data/spark`).

## The retention requirement (a real bug this caught)

Spark silently discards rows older than its watermark. The watermark follows the newest Kafka
timestamp minus `--dedup-retention-hours` (default 24). In the first full demo run (a 3-day
replay, default retention) Spark read the small `customers` partitions slowly while the large
topics raced days ahead, then dropped the last 15 customer messages as too old. The job
reported success with 1,475 of 1,490 messages, because the earlier gap check only looked
inside a batch. The earlier integration test missed it because its window (18 h) was shorter
than the retention.

What changed: each batch is now verified against Spark's planned offsets (start and end per
partition, from the checkpoint), so a drop anywhere, including a whole partition delivering
nothing, fails the batch loudly. The corrected demo, with `--dedup-retention-hours 96`,
processed all 1,490 messages (checked partition by partition) and is covered by a regression
test that forces a too-short retention and expects a loud failure.

A second symptom of the same misconfiguration, found while testing the regression: with a retention
much shorter than the data's time spread, Spark refuses a state expiry that lies behind its
watermark (`INVALID_TIMEOUT_TIMESTAMP`) and the query dies. The job now clamps the expiry just past
the watermark, so the state simply expires early, and the offset check above still turns any dropped
rows into one explained failure. Either way the job ends with a single `query failed` log line, exit
code 1 and the messages still in Kafka; it never reports success with fewer messages than planned.

Rule of thumb: the retention must be at least the time span of the data being read at once.
With live traffic, Kafka timestamps are arrival times and partitions stay roughly in step, so
the default is fine; replaying a historical window needs its full length.

## Performance and resources (measured, not tuned)

On the 8 GB development machine with a ~5 GB Docker VM, a 394-message micro-batch took
between about 2 and 4 minutes while the machine was under memory pressure, plus a
one-time start-up of about a minute. Most of this is JVM and Python-worker start-up and
Python UDFs, not throughput of the design. The earlier runs on a less loaded machine
needed 20 to 80 seconds per batch. No throughput benchmark has been done and none is claimed.

If Kafka reports `unhealthy` or `docker` commands time out, the Docker VM is starved:
stop Kafka UI (`docker compose stop kafka-ui`) and do not run other heavy containers
alongside Spark. A starved broker can stop heartbeating its controller and needs
`docker compose restart kafka` (its data is on a volume and survives).

## Not covered

- Real AWS S3 and GCS as destinations. MinIO is verified as the S3 side; GCS is a local directory.
- Multiple executors or a Spark cluster; the job runs in `local[2]`.
- Executor-side parallel writes: the driver commits each micro-batch (see ADR 0011).
- Spark driver metrics in Prometheus (the exporter publishes Spark's per-batch progress and lag, see
  [monitoring.md](monitoring.md), but the one-shot container has no scrape target of its own).
- The Airflow DAGs and the metrics-exporter container still read local directories, not MinIO.
