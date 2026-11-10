# 7. Portfolio and Demo Specification

## 7.1 What the project should prove

A reviewer should be able to see evidence of: - Event-driven data
ingestion with Kafka. - Stream processing and validation with Spark. -
Multi-cloud object storage using AWS and GCP. - Warehouse loading and
analytical modelling in BigQuery. - dbt transformations, tests and
lineage. - Airflow orchestration and backfills. - Terraform-managed
infrastructure. - Data quality, idempotency, replay and operational
monitoring. - Clear documentation and reproducible development.

## 7.2 Required portfolio assets

-   Architecture diagram (logical and deployed topology).
-   Data-flow diagram and event contract examples.
-   Repository README with quick-start commands.
-   Data dictionary and warehouse model diagram.
-   Screenshot of Kafka topics/messages.
-   Screenshot of Spark processing and quality metrics.
-   Screenshot of S3 and GCS partitioned objects.
-   Screenshot of BigQuery tables and dbt lineage/docs.
-   Screenshot of Airflow successful DAG run.
-   Grafana operational dashboard.
-   BI dashboard with synthetic business metrics.
-   CI run showing passing checks.
-   Short demo recording (3--6 minutes).

Do not fabricate screenshots or claim cloud deployment if only local
mode was run.

## 7.3 Suggested business dashboard

1.  Revenue/net sales over time.
2.  Orders per day and average order value.
3.  Payment success/failure/refund rate.
4.  Top products and categories by units and net sales.
5.  Inventory on hand and low-stock products.
6.  Data freshness and last successful warehouse load.

Use the documented metric definitions. Label all data as synthetic/demo
data.

## 7.4 Demo script

1.  Show the architecture and explain why AWS and GCP both exist.
2.  Start local services.
3.  Generate a small batch of synthetic commerce events.
4.  Show messages in Kafka.
5.  Show Spark processing counts and quarantine behavior.
6.  Show raw objects and batch manifest.
7.  Run the warehouse load and dbt build.
8.  Open the sales and payment marts/dashboard.
9.  Trigger a duplicate or malformed event and show the quality result.
10. Explain idempotent replay and the limits of the current cross-cloud
    design.

## 7.5 Resume/project description template

**Multi-Cloud Real-Time E-Commerce Data Platform**\
Designed and implemented an event-driven data engineering platform using
Python, Kafka, PySpark, AWS S3, Google Cloud Storage, BigQuery, dbt,
Airflow and Terraform. Built versioned event ingestion, validation and
quarantine, idempotent warehouse loading, cross-cloud data
reconciliation, analytics marts and pipeline monitoring.

Only include technologies and outcomes that are actually implemented and
tested. Add measured throughput, dataset size, latency or cost only
after benchmarking and recording the test conditions.

## 7.6 Interview questions to prepare

-   Why use Kafka instead of direct database writes?
-   What is the difference between event time and ingestion time?
-   How do you handle duplicate and late events?
-   Why is end-to-end exactly-once difficult across multiple systems?
-   Why store raw data in object storage and curated data in a
    warehouse?
-   What does dbt add after data lands in BigQuery?
-   How does Airflow differ from Spark?
-   How do you make a backfill idempotent?
-   How do you detect S3/GCS replication mismatch?
-   What happens if one cloud destination is unavailable?
-   How do you control BigQuery query costs?
-   What is managed by Terraform and what is managed by application
    code?

## 7.7 Stretch goals (only after version 1 is complete)

-   Schema registry and compatibility enforcement.
-   Change Data Capture (CDC) from a sample PostgreSQL commerce
    database.
-   SCD Type 2 customer/product dimensions.
-   Data lineage integration beyond dbt docs.
-   Alert delivery to Slack/email.
-   A real managed streaming service comparison (for example Dataflow or
    a managed Kafka offering).
-   Tested recovery/failover with explicit RPO/RTO.
-   Data contract CI and automated schema compatibility tests.
