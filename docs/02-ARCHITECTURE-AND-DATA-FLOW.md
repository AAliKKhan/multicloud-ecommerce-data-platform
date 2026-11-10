# 2. Architecture and Data Flow

## 2.1 Logical architecture

``` mermaid
flowchart LR
    A[Python synthetic commerce generator] --> B[Kafka topics]
    B --> C[PySpark Structured Streaming]
    C --> D{Validate and normalize}
    D -->|Valid| E[Raw event files]
    D -->|Invalid| Q[Quarantine / dead-letter]
    E --> S3[AWS S3 raw zone]
    E --> GCS[Google Cloud Storage raw zone]
    S3 --> GLUE[AWS Glue Data Catalog]
    S3 --> L[Batch load / Spark]
    GCS --> R[Cloud parity reconciliation]
    L --> BQ[BigQuery]
    BQ --> DBT[dbt models]
    DBT --> MART[Analytics marts]
    MART --> BI[Metabase / Looker Studio]
    AF[Airflow] -. orchestrates .-> L
    AF -. orchestrates .-> DBT
    AF -. orchestrates .-> R
    P[Prometheus] --> GR[Grafana]
    C -. metrics .-> P
    AF -. metrics/logs .-> P
```

## 2.2 Runtime modes

**Local mode (mandatory first):** - Docker Compose starts Kafka, Kafka
UI, Spark, Airflow, PostgreSQL metadata DB, Prometheus, Grafana and a
local S3-compatible object store such as MinIO. - Use a local filesystem
or MinIO bucket for the GCS adapter during early development, but label
this as a local substitute. Do not claim GCS is running locally. - Use a
local warehouse-compatible test strategy for unit tests; BigQuery
integration tests require explicit cloud credentials.

**Cloud mode (optional):** - AWS S3 is the primary raw landing
destination. - GCS receives a replicated raw copy. - Glue catalogues the
AWS raw zone. - BigQuery hosts analytical tables and dbt models. -
Airflow and Spark can remain containerized for the portfolio release.
Document where they run and their networking requirements.

## 2.3 Event path

1.  Generator creates an event with `event_id`, `event_type`,
    `schema_version`, `event_time`, `producer`, and payload.
2.  Producer publishes to the appropriate Kafka topic, using `event_id`
    as the message key.
3.  Spark reads the topic, parses the envelope, validates schema and
    casts fields.
4.  Valid events are written in append-only files, partitioned by event
    date and hour.
5.  Invalid events are written to quarantine with error code, error
    detail, original payload and processing time.
6.  Each micro-batch writes a manifest containing batch ID, time range,
    input count, valid count, invalid count and destination status.
7.  A batch loader reads raw partitions and performs idempotent
    `MERGE`/upsert logic into BigQuery staging tables.
8.  dbt creates typed staging models, intermediate business logic and
    marts.
9.  Airflow runs scheduled batch loading, dbt, quality tests and
    reconciliation.
10. BI tools query curated marts, not raw event files.

## 2.4 Kafka topic plan

  -----------------------------------------------------------------------
  Topic                   Key                     Event types
  ----------------------- ----------------------- -----------------------
  `commerce.orders`       order_id                order_created,
                                                  order_updated,
                                                  order_cancelled

  `commerce.payments`     payment_id              payment_authorized,
                                                  payment_captured,
                                                  payment_failed,
                                                  payment_refunded

  `commerce.inventory`    product_id +            stock_received,
                          location_id             stock_reserved,
                                                  stock_adjusted

  `commerce.customers`    customer_id             customer_created,
                                                  customer_updated
  -----------------------------------------------------------------------

Use separate topics for dead-letter events, or a unified `commerce.dlq`
topic with original topic and partition metadata. Configure replication
factor according to the actual Kafka deployment; a single-node local
demo cannot provide broker fault tolerance.

## 2.5 Storage zones

-   **Raw / bronze:** immutable event payloads, preserving source
    structure.
-   **Validated / silver:** normalized types, deduplicated event records
    and explicit quality status.
-   **Curated / gold:** business-level facts, dimensions and aggregates
    in BigQuery.

Suggested object key:

`raw/<source>/<event_type>/event_date=YYYY-MM-DD/event_hour=HH/part-<run_id>-<partition>.jsonl`

Use Parquet for validated/curated file datasets where appropriate. Keep
raw JSON for replay and schema evolution.

## 2.6 Cross-cloud replication and parity

For each batch: - Write the same logical batch to S3 and GCS. - Generate
a manifest with batch ID, event count, minimum/maximum event time,
object keys and a deterministic checksum strategy. - Reconcile counts
and checksums at the logical-record level. - Mark a batch `COMPLETE`
only when both required destinations have succeeded, or record a partial
status and retry the failed destination. - Make retries safe by using
deterministic object names or a manifest/commit protocol.

Do not describe this as automatic failover. Version 1 is dual-write
replication with reconciliation. A future failover feature would require
health detection, a source-of-truth decision, replay strategy and tested
recovery objectives.

## 2.7 Failure handling

  -----------------------------------------------------------------------
  Failure                             Expected behavior
  ----------------------------------- -----------------------------------
  Kafka temporarily unavailable       Producer retries with bounded
                                      backoff; no silent discard

  Malformed event                     Quarantine with reason and original
                                      payload

  Spark restart                       Resume from checkpoint; document
                                      checkpoint storage

  S3 write fails                      Retry; mark batch partial; do not
                                      mark committed

  GCS write fails                     Retry independently; parity status
                                      remains incomplete

  BigQuery load fails                 Keep raw files; retry idempotent
                                      load

  dbt test fails                      Fail DAG task and preserve
                                      logs/artifacts

  Duplicate event                     Deduplicate by stable event ID;
                                      record duplicate metric

  Late event                          Process based on event-time policy
                                      and support backfill

  Airflow unavailable                 Streaming can continue if
                                      independently deployed; scheduled
                                      loads wait
  -----------------------------------------------------------------------

## 2.8 Processing guarantees

Kafka producer idempotence and Spark checkpointing improve reliability
but do not create end-to-end exactly-once behavior across two cloud
object stores and BigQuery. Design for **at-least-once delivery plus
idempotent writes and deduplication**. State this accurately in
documentation.
