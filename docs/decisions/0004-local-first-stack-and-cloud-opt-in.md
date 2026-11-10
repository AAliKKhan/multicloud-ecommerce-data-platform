# ADR 0004: Local-first development, cloud strictly opt-in

- Status: accepted
- Date: 2026-11-10

## Context

The project owner currently has no AWS or GCP account or credits, and Docker
is not installed on the development machine yet.

## Decision

- Build pure-Python parts first (generator, config, validation, storage
  interface, loader logic) and test them without Docker or cloud access.
- Docker-based services (Kafka KRaft, Spark, Airflow, MinIO, Prometheus,
  Grafana) are integrated last.
- Python is pinned to 3.12 (`>=3.11,<3.13`): PySpark, Airflow and
  dbt-bigquery do not reliably support newer interpreters.
- Locally, MinIO or the filesystem stand in for S3 and GCS. They are labelled
  as local substitutes; GCS is never claimed to run locally.
- BigQuery and real AWS/GCP code paths are implemented behind interfaces with
  fakes in tests. They are marked **unverified** until real credentials exist.
- No `terraform apply`, no cloud uploads and no billable resources without
  explicit approval.
- Python is the first-class runner (`uv run ...`); a task runner is added
  later because `make` is not available on the Windows dev machine.
