# 6. Operations, Security and Cost

## 6.1 Security baseline

-   Use synthetic data only.
-   Never collect/store payment card numbers, CVVs, passwords or real
    customer PII.
-   Store local secrets in an ignored `.env`; use cloud secret managers
    or workload identity in cloud environments.
-   Use least-privilege IAM roles and service accounts.
-   Block public access to object-storage buckets.
-   Enable encryption at rest and TLS in transit where supported.
-   Restrict Airflow, Kafka UI, MinIO, Grafana and BI interfaces to
    local/private access in development.
-   Do not log credentials, tokens or full sensitive payloads.
-   Pin dependencies and scan for known vulnerabilities.
-   Protect GitHub Actions secrets and use minimal workflow permissions.

## 6.2 Observability metrics

At minimum expose: - Kafka messages produced/consumed and consumer
lag. - Spark input rows, processed rows, processing duration and batch
failures. - Valid, invalid, duplicate and late event counts. -
DLQ/quarantine rate. - Object write latency and failures by
destination. - S3/GCS batch count parity. - BigQuery load row counts,
duration and failures. - Airflow DAG/task duration, retries and
failures. - Data freshness and last successful batch timestamp. -
Business totals such as orders and net sales, clearly separated from
infrastructure metrics.

Use low-cardinality metric labels. Do not label metrics with customer
IDs, order IDs or other unbounded identifiers.

## 6.3 Operational runbooks

### Pipeline is stale

1.  Check Airflow DAG and latest task logs.
2.  Check Kafka consumer lag and Spark query status.
3.  Check destination credentials, bucket access and quota.
4.  Identify the last committed batch manifest.
5.  Retry from the last uncommitted batch; do not blindly rerun all
    data.
6.  Verify warehouse counts and dbt freshness after recovery.

### Cloud destination partially failed

1.  Mark the batch partial, not complete.
2.  Keep the successful destination intact.
3.  Retry the failed destination using the same batch ID and
    deterministic object keys.
4.  Re-run parity reconciliation.
5.  Commit the batch only after the required destinations agree.

### Duplicate records detected

1.  Identify the source topic/partition/offset and event IDs.
2.  Confirm deduplication key and schema version.
3.  Re-run idempotent warehouse merge.
4.  Confirm mart totals have not increased.
5.  Add a regression test.

### Bad schema deployment

1.  Stop or pause the affected consumer if necessary.
2.  Preserve original raw payloads.
3.  Identify incompatible schema version.
4.  Add backward-compatible parsing or a new versioned schema.
5.  Replay quarantined events after validation.

## 6.4 Recovery and replay

-   Raw data is the replay source.
-   Store manifests and checkpoints durably.
-   Document the difference between restarting a Spark query and
    replaying a historical date range.
-   Backfill must accept start and end dates, be idempotent and produce
    a run report.
-   Define recovery objectives for the demo; do not claim enterprise
    RPO/RTO without measurement.
-   Cross-cloud copies are not a backup unless retention, deletion
    protection and restore procedures are tested.

## 6.5 Cost controls

Cloud costs depend on region, data volume, retention, query patterns and
current provider pricing. Do not hardcode cost claims.

-   Keep cloud deployment disabled by default.
-   Use small synthetic datasets and low event rates.
-   Set lifecycle/retention policies for demo buckets.
-   Use BigQuery partition filters and avoid `SELECT *` in routine
    analytics.
-   Avoid frequent full-table scans and unnecessary crawlers.
-   Use budget alerts where available.
-   Provide a teardown checklist and verify that billable resources are
    removed.
-   Distinguish free-tier eligibility from guaranteed free usage; quotas
    and pricing can change.
-   Never run Terraform apply or create paid resources without explicit
    user authorization.

## 6.6 Environment separation

-   `local`: Docker Compose and synthetic fixtures.
-   `dev-cloud`: isolated test buckets/dataset and limited data volume.
-   `prod-like`: optional demonstration environment with stricter IAM
    and monitoring.

Do not reuse production credentials in local development. Use separate
bucket names, datasets and service identities per environment.
