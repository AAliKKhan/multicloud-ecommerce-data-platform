# Multi-Cloud E-Commerce Data Platform

A portfolio-grade data engineering project: synthetic e-commerce events flow
through **Kafka** and **PySpark Structured Streaming**, land as raw data in
**AWS S3** and **Google Cloud Storage**, load into **BigQuery**, are modelled
with **dbt**, orchestrated by **Airflow**, and monitored with **Prometheus**
and **Grafana**. Infrastructure is described with **Terraform**.

> **Synthetic data only.** No real customers, payments or card data are ever
> used. Cloud deployment is opt-in; the whole pipeline is designed to run
> locally without cloud credentials.

## Status

This project is being built milestone by milestone. Nothing below is claimed
as working until its acceptance test passes.

| Milestone | Scope | Status |
|---|---|---|
| M0 | Repository, config validation, ADRs, CI baseline | In progress |
| M1 | Seeded synthetic commerce generator | Planned |
| M2 | Kafka producer and local broker (Docker) | Planned |
| M3 | Spark streaming validation, dedup, quarantine | Planned |
| M4 | Storage abstraction, manifests, cross-cloud parity | Planned |
| M5 | Terraform for AWS and GCP (plan only, never applied by default) | Planned |
| M6 | Idempotent warehouse loader (BigQuery `MERGE`) | Planned |
| M7 | dbt staging, intermediate and mart models | Planned |
| M8 | Airflow DAGs | Planned |
| M9 | Data quality and observability | Planned |
| M10 | BI dashboard and portfolio polish | Planned |

Docker-dependent milestones are built last; Python-only parts come first and
are tested without Docker.

## Design principles

- At-least-once delivery plus idempotent writes and deduplication. The project
  does **not** claim exactly-once delivery or automatic cross-cloud failover.
- Raw events are immutable and are the replay source.
- Every event carries an ID, schema version, event time and ingestion time.
- Duplicate, late, malformed and out-of-order events are handled explicitly.
- Secrets never live in source control.

## Documentation

The full specification is in [`docs/`](docs/README.md):

1. [Product and system requirements](docs/01-PRODUCT-AND-SYSTEM-REQUIREMENTS.md)
2. [Architecture and data flow](docs/02-ARCHITECTURE-AND-DATA-FLOW.md)
3. [Tech stack and infrastructure](docs/03-TECH-STACK-AND-INFRASTRUCTURE.md)
4. [Data contracts and transformations](docs/04-DATA-CONTRACTS-AND-TRANSFORMATIONS.md)
5. [Implementation plan](docs/05-IMPLEMENTATION-PLAN-AND-AGENT-INSTRUCTIONS.md)
6. [Operations, security and cost](docs/06-OPERATIONS-SECURITY-AND-COST.md)
7. [Portfolio and demo](docs/07-PORTFOLIO-AND-DEMO.md)

## License

MIT
