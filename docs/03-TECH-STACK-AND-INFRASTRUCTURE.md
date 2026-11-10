# 3. Technology Stack and Infrastructure

## 3.1 Selected stack

  -----------------------------------------------------------------------
  Layer                   Technology              Responsibility
  ----------------------- ----------------------- -----------------------
  Language                Python 3.11+            Generator, ingestion
                                                  utilities, tests and
                                                  orchestration helpers

  Event broker            Apache Kafka            Durable event transport

  Stream processing       PySpark Structured      Parse, validate,
                          Streaming               deduplicate and land
                                                  events

  AWS object storage      Amazon S3               Primary raw event
                                                  archive

  AWS catalog             AWS Glue Data Catalog   Table and partition
                                                  metadata for S3 data

  GCP object storage      Google Cloud Storage    Second raw-data copy

  Analytics warehouse     BigQuery                Analytical storage and
                                                  SQL execution

  Transformations         dbt Core + BigQuery     Staging, intermediate
                          adapter                 and mart models

  Orchestration           Apache Airflow          Batch scheduling,
                                                  backfills and
                                                  dependency management

  Infrastructure          Terraform               Provision cloud
                                                  buckets, IAM and
                                                  required GCP resources

  Local object storage    MinIO                   S3-compatible local
                                                  development target

  Quality                 Great Expectations or   Validation and quality
                          dbt tests + custom      reporting
                          checks                  

  Metrics                 Prometheus              Metrics collection

  Dashboards              Grafana                 Operational monitoring

  BI                      Metabase or Looker      Business analytics
                          Studio                  

  CI/CD                   GitHub Actions          Automated quality and
                                                  build checks

  Packaging               Docker / Docker Compose Reproducible local
                                                  services
  -----------------------------------------------------------------------

Keep the first release lean. Great Expectations is optional if dbt tests
and explicit Python validation cover the acceptance criteria. Do not
introduce Dataflow, Kubernetes or multiple orchestration systems just to
increase the tool count.

## 3.2 Repository structure

``` text
multicloud-ecommerce-platform/
├── README.md
├── docs/
│   ├── architecture.md
│   ├── data-dictionary.md
│   ├── operations.md
│   └── decisions/
├── config/
│   ├── local.example.yaml
│   └── cloud.example.yaml
├── infra/
│   └── terraform/
│       ├── aws/
│       └── gcp/
├── docker/
├── docker-compose.yml
├── generator/
│   ├── src/
│   └── tests/
├── producer/
│   ├── src/
│   └── tests/
├── spark/
│   ├── jobs/
│   ├── schemas/
│   └── tests/
├── storage/
│   ├── interfaces/
│   ├── local/
│   ├── aws/
│   └── gcp/
├── warehouse/
│   ├── loaders/
│   └── sql/
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── tests/
│   ├── macros/
│   └── dbt_project.yml
├── airflow/
│   └── dags/
├── monitoring/
│   ├── prometheus/
│   └── grafana/
├── tests/
│   ├── integration/
│   └── e2e/
└── .github/workflows/
```

The agent may refine names, but preserve clear separation between
generation, ingestion, processing, storage adapters, warehouse loading,
transformations and orchestration.

## 3.3 Configuration

Use environment variables or typed YAML configuration. Provide
`.env.example`, never commit `.env`.

Example configuration keys: - `APP_ENV=local` -
`KAFKA_BOOTSTRAP_SERVERS=localhost:9092` -
`KAFKA_TOPIC_PREFIX=commerce` - `AWS_REGION` - `AWS_S3_BUCKET` -
`GCP_PROJECT_ID` - `GCP_REGION` - `GCS_BUCKET` - `BIGQUERY_DATASET` -
`AIRFLOW__CORE__EXECUTOR` - `LOG_LEVEL=INFO`

Use separate local and cloud configuration profiles. Validate required
settings at startup and fail with actionable messages.

## 3.4 Docker Compose

Compose should provide: - Kafka and its required controller/broker
services (choose a supported KRaft configuration). - Kafka UI. - Spark
master/worker or a documented single-node Spark deployment. - Airflow
webserver, scheduler and metadata PostgreSQL. - MinIO and its console
for local object storage. - Prometheus and Grafana. - Optional Metabase.

Pin tested image versions; avoid `latest`. Add health checks, named
volumes, restart policies and resource guidance. Use non-default
credentials from local environment variables. Document that Compose is a
development topology, not a highly available production cluster.

## 3.5 Terraform scope

AWS resources: - S3 raw bucket with public access blocked, encryption,
versioning where justified, lifecycle rules and least-privilege IAM
policy. - Glue database and crawler/table definitions only if useful;
avoid running crawlers unnecessarily.

GCP resources: - GCS bucket with uniform bucket-level access, encryption
defaults, versioning/lifecycle where justified. - BigQuery dataset with
location configured consistently. - Service account and narrowly scoped
IAM bindings.

Terraform requirements: - Separate AWS and GCP providers/configurations
cleanly. - Use variables for project, region, bucket names and
environment. - Include outputs for resource names, not secrets. - State
must be remote for shared/team use; local state is acceptable only for
an explicitly documented personal sandbox. - Add `.gitignore` for state
files, plans and credentials. - Require explicit approval before
`terraform apply` or any billable cloud provisioning. - Provide
`terraform plan`, `apply` and `destroy` instructions. - Never hardcode
access keys or service-account JSON.

## 3.6 Cloud adapters

Define a small storage interface with operations such as: -
`write_object(key, bytes, metadata)` - `read_object(key)` -
`list_objects(prefix)` - `object_exists(key)`

Implement local/MinIO, S3 and GCS adapters. Keep provider-specific SDK
code out of Spark business transformations. Unit-test adapters with
fakes or emulators where practical; run real cloud integration tests
only when explicitly configured.

## 3.7 Dependency and version management

-   Pin Python dependencies in a lock file or constraints file.
-   Pin Docker image tags.
-   Document supported Python, Spark, Kafka, Airflow and dbt versions.
-   Avoid unbounded dependencies and deprecated APIs.
-   Add formatting/linting (Ruff), type checking (mypy or pyright where
    practical), and pytest.
-   Record architecture decisions for material choices.
