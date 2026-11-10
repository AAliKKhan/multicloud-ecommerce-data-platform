# 5. Implementation Plan and Coding-Agent Instructions

## 5.1 Instructions to the coding agent

You are the lead data engineer implementing this specification.

1.  Read all files in this `docs` folder before coding.
2.  Inspect the existing repository before creating or overwriting
    files.
3.  First produce a short implementation plan and list any conflicts or
    missing decisions.
4.  Build in milestones. After each milestone, run tests and report
    exact commands and outcomes.
5.  Do not generate a huge untested code dump.
6.  Do not use placeholder functions for core requirements. If a cloud
    integration cannot be completed without credentials, implement the
    interface, a clear configuration error, tests with fakes, and
    document the remaining cloud verification.
7.  Never create billable resources, run `terraform apply`, or upload
    data to cloud services without explicit user approval.
8.  Never commit secrets, state files, `.env` files or service-account
    credentials.
9.  Keep local mode fully functional without AWS/GCP credentials.
10. Update docs and `.env.example` whenever configuration changes.
11. Do not claim a feature is complete until its acceptance test passes.
12. Prefer simple, well-supported components over unnecessary
    complexity.

## 5.2 Milestone 0 --- Repository and decisions

-   Inspect repository, current branch, runtime and existing
    conventions.
-   Create or update the directory structure.
-   Add architecture decision records for event delivery semantics,
    storage formats, cloud locations and local stack choices.
-   Define environment/config validation.
-   Add `.gitignore`, `.env.example`, Python packaging and baseline CI.

**Exit criteria:** clean install instructions, configuration validation
test, CI baseline passing.

## 5.3 Milestone 1 --- Synthetic commerce generator

-   Implement seeded generation for customers, products, orders, items,
    payments and inventory events.
-   Support configurable event rate, total count, time range and
    invalid-event rate.
-   Generate deterministic IDs for repeatable fixtures where
    appropriate; use unique IDs for normal runs.
-   Include valid, duplicate, malformed, late and out-of-order test
    cases.
-   Add unit tests for schema and business constraints.

**Exit criteria:** CLI can generate a fixture dataset and event stream;
tests validate all event types.

## 5.4 Milestone 2 --- Kafka producer and local broker

-   Add Docker Compose Kafka in KRaft mode and Kafka UI.
-   Create topics and document partition/key choices.
-   Implement producer with message key, delivery callback, bounded
    retry and structured logging.
-   Add graceful shutdown and producer health check.
-   Add integration test that publishes and consumes sample events.

**Exit criteria:** generated events can be observed on each topic;
failed publishing is visible and not silently ignored.

## 5.5 Milestone 3 --- Spark stream processing

-   Implement schemas and parsing.
-   Validate envelope and event payload.
-   Normalize timestamps and numeric types.
-   Deduplicate by `event_id` using checkpoint-aware logic.
-   Write valid and quarantined events separately.
-   Add checkpoint location configuration and restart test.
-   Export metrics: input rows, valid rows, invalid rows, duplicate
    rows, processing latency.

**Exit criteria:** valid/invalid/duplicate fixtures are handled
correctly; restart does not cause business double-counting.

## 5.6 Milestone 4 --- Local dual-storage abstraction

-   Implement storage interface.
-   Implement MinIO/S3-compatible adapter and local filesystem adapter.
-   Implement GCS adapter using official SDK, with credential checks.
-   Write partitioned raw objects and batch manifests.
-   Implement parity reconciliation.
-   Test partial failure and retry behavior with fakes.

**Exit criteria:** local run writes durable objects/manifests; adapter
contract tests pass; cloud adapter tests are gated.

## 5.7 Milestone 5 --- AWS and GCP infrastructure

-   Add Terraform for S3, Glue catalog resources, GCS and BigQuery
    dataset/IAM.
-   Add least-privilege IAM policies and encryption/access controls.
-   Add variables and outputs; document regions and location
    compatibility.
-   Add plan validation to CI without applying.
-   Test `terraform plan` only after user configures credentials and
    approves.

**Exit criteria:** validated Terraform plans; no secrets in repository;
deployment and teardown docs complete.

## 5.8 Milestone 6 --- Warehouse loading

-   Implement raw-to-BigQuery batch loader.
-   Load to staging tables with explicit schemas.
-   Use deterministic batch IDs and idempotent `MERGE`.
-   Record load manifests and reconciliation counts.
-   Support date-range backfill.
-   Add integration tests for duplicate batch replay.

**Exit criteria:** replaying a batch does not duplicate warehouse facts;
load status and counts are auditable.

## 5.9 Milestone 7 --- dbt transformations

-   Configure dbt Core and BigQuery adapter.
-   Build staging, intermediate and mart models listed in the data
    contract.
-   Add schema tests: unique, not-null, relationships, accepted values.
-   Add singular tests for reconciliation and metric logic.
-   Generate dbt docs and lineage.
-   Add seed data for local/test development if useful.

**Exit criteria:** `dbt build` passes against configured BigQuery test
dataset; models and metric definitions documented.

## 5.10 Milestone 8 --- Airflow orchestration

Create DAGs for: - `raw_to_warehouse_daily` - `dbt_transform_and_test` -
`cross_cloud_reconciliation` - `historical_backfill` (parameterized date
range)

DAG requirements: - Retries with bounded delay. - Sensible timeouts and
concurrency limits. - Idempotent tasks. - Run IDs and structured logs. -
No secrets embedded in DAG source. - Clear failure notifications/log
guidance. - Streaming process lifecycle documented separately.

**Exit criteria:** DAGs visible in Airflow and a complete synthetic
batch workflow runs successfully.

## 5.11 Milestone 9 --- Data quality and observability

-   Implement quality result records and quarantine inspection workflow.
-   Add Prometheus metrics endpoint or supported exporter.
-   Add Grafana dashboards for Kafka, Spark, Airflow and pipeline
    business metrics.
-   Add alerts for stale pipeline, repeated failures, high DLQ rate and
    cloud parity mismatch.
-   Verify dashboards using a deliberately failing test scenario.

**Exit criteria:** operational dashboard shows live metrics and detects
a simulated pipeline failure.

## 5.12 Milestone 10 --- BI and portfolio polish

-   Build a Metabase or Looker Studio dashboard over curated marts.
-   Add architecture diagram, data dictionary, screenshots and demo
    dataset.
-   Write clean setup, cloud deployment, teardown and troubleshooting
    docs.
-   Record a short demo showing data flowing end to end.
-   Add a CI workflow and a release checklist.

**Exit criteria:** a reviewer can clone, start local mode, generate
data, inspect outputs and understand the design without private
assistance.

## 5.13 Suggested command interface

Provide a consistent CLI, for example:

``` bash
make up
make down
make generate
make topics
make produce
make spark
make test
make dbt-build
make airflow
make demo
```

Commands may differ, but document exact equivalents. `make demo` should
not require cloud credentials.

## 5.14 Testing strategy

-   Unit: schema parsing, validation, generators, metric calculations,
    idempotency keys.
-   Contract: producer/consumer schema compatibility.
-   Integration: Kafka publish/consume, Spark processing, object storage
    adapter.
-   Warehouse: staging loads, merge idempotency, dbt tests and
    reconciliation.
-   End-to-end: generate -\> Kafka -\> Spark -\> raw storage -\>
    warehouse -\> dbt mart.
-   Failure: malformed event, duplicate, late event, destination outage,
    retry and restart.
-   Security: secret scanning, dependency scanning and Terraform checks.

## 5.15 Final definition of done

The project is done only when: - All required source code is
implemented. - Local demo works from a clean checkout. - Tests pass and
their results are reported. - Cloud integrations are either verified or
explicitly marked unverified due to missing credentials. - Terraform
plans are validated; no resources were applied without approval. - Data
contracts and metric definitions are documented. - Dashboard and
operational metrics are demonstrated. - Setup, backfill, recovery and
teardown instructions are accurate. - No secrets or real
personal/payment data are present.
