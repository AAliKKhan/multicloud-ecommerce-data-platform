# 1. Product and System Requirements

## 1.1 Product summary

Build a synthetic e-commerce data platform that demonstrates multi-cloud
data engineering. It should ingest order, payment, customer and
inventory events; stream them through Kafka; process and validate them
with Spark; land raw data in AWS S3 and Google Cloud Storage; load
curated data into BigQuery; transform it with dbt; orchestrate jobs with
Airflow; and expose operational and business analytics.

## 1.2 Goals

-   Demonstrate event-driven ingestion and batch processing.
-   Demonstrate storage and analytics across AWS and GCP.
-   Demonstrate data modelling, quality checks, orchestration and
    monitoring.
-   Demonstrate infrastructure-as-code and repeatable local development.
-   Provide a clear, credible technical portfolio and a live demo.

## 1.3 Out of scope for version 1

-   Real payment processing or card data.
-   Real customer PII or production commerce integrations.
-   Multi-region active-active architecture.
-   Guaranteed exactly-once semantics across Kafka, object storage and
    BigQuery.
-   Kubernetes, service mesh or complex microservices.
-   A public SaaS product with authentication and tenant billing.
-   Automatic cloud failover that has not been implemented and tested.

## 1.4 Users and use cases

**Data engineer:** inspect pipeline health, replay a date range,
identify failed records and verify data quality.

**Business analyst:** explore sales, orders, payment status, product
performance and inventory trends.

**Portfolio reviewer / interviewer:** run the stack locally, inspect
code and infrastructure, review tests, and understand the cloud design.

## 1.5 Functional requirements

### FR-01 --- Synthetic event generation

Generate realistic but fictional customers, products, orders, order
items, payments and inventory updates. Configurable event rate, date
range, seed and error rate. Include a deterministic seed for repeatable
tests.

### FR-02 --- Event streaming

Publish versioned JSON events to Kafka topics. Use a stable event ID as
the deduplication key. Provide producer retries and structured logs.

### FR-03 --- Stream processing

Consume Kafka events with PySpark Structured Streaming. Validate
required fields, normalize timestamps and types, detect duplicates, and
route invalid events to a dead-letter topic or quarantine output.

### FR-04 --- Raw multi-cloud landing

Write append-only raw event files to S3 and GCS in date/hour partitions.
Store the same logical event set in both destinations. Record per-batch
manifests and counts so parity can be checked.

### FR-05 --- Catalog and warehouse

Register the AWS raw-data location in Glue Data Catalog. Load curated
data into BigQuery using a documented, repeatable method. The initial
release may use Spark batch jobs to load BigQuery; do not add Dataflow
unless it is genuinely implemented.

### FR-06 --- Transformations

Use dbt to build staging, intermediate and mart models in BigQuery.
Include tests, documentation and lineage.

### FR-07 --- Orchestration

Use Airflow DAGs to coordinate batch backfills, raw-to-warehouse
loading, dbt runs, quality checks and reconciliation. Streaming services
may run continuously outside the scheduled DAG.

### FR-08 --- Data quality

Check schema validity, uniqueness, nullability, referential integrity,
accepted values, freshness, row counts and cross-cloud parity. Store
results and surface failures.

### FR-09 --- Analytics

Provide dashboards for revenue, order volume, average order value,
payment outcomes, top products, inventory risk and pipeline health.

### FR-10 --- Observability

Expose pipeline metrics and structured logs. Provide Prometheus scraping
and Grafana dashboards for service and pipeline health.

### FR-11 --- Infrastructure as code

Provision the required AWS and GCP resources with Terraform. Separate
development and production-like configuration. Cloud resources must not
be created by default.

### FR-12 --- CI

Run formatting, linting, unit tests, dbt validation and Docker build
checks in GitHub Actions. Cloud integration tests should be optional and
use protected secrets.

## 1.6 Non-functional requirements

-   **Reliability:** retries with bounded backoff; failed records are
    retained for diagnosis.
-   **Idempotency:** reprocessing a batch must not inflate business
    totals.
-   **Maintainability:** modular code, typed configuration, clear
    interfaces and documented ownership.
-   **Portability:** core pipeline runs locally without cloud
    credentials.
-   **Security:** least-privilege service accounts, secret management
    and no committed credentials.
-   **Performance:** support a configurable demo rate (for example
    10--100 events/second locally); report measured throughput rather
    than claiming a target is achieved.
-   **Cost:** cloud resources are opt-in, budgeted and easy to destroy.
-   **Auditability:** every batch has a run ID, input range, output
    locations, counts and status.

## 1.7 Acceptance criteria

A release is acceptable when: 1. A developer can start the local stack
from documented commands. 2. Synthetic events appear in Kafka and are
processed by Spark. 3. Valid events land in partitioned local
object-storage-compatible output or configured cloud buckets. 4. Invalid
events are quarantined with a reason and source payload. 5. Re-running a
batch does not double-count orders or payments. 6. dbt builds documented
BigQuery models and its tests pass. 7. Airflow can run a backfill and
report success/failure. 8. Data quality and cloud-parity reports are
generated. 9. Grafana shows meaningful service/pipeline metrics. 10. CI
passes on a clean checkout. 11. A demo can be completed using synthetic
data only. 12. Teardown instructions are tested and documented.
