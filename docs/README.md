# Multi-Cloud E-Commerce Data Platform

**Purpose:** Build a production-style, portfolio-grade data engineering
platform that ingests e-commerce events, processes them in near real
time and in batches, stores durable raw data across AWS and Google
Cloud, transforms trusted datasets, and exposes analytics.

This repository is an implementation specification for an AI coding
agent. Treat the documents in this folder as the source of truth.
Implement in small, testable milestones; do not skip acceptance criteria
or replace real integrations with mock implementations without clearly
marking them.

## Document map

  ----------------------------------------------------------------------------------------
  File                                                 Purpose
  ---------------------------------------------------- -----------------------------------
  `01-PRODUCT-AND-SYSTEM-REQUIREMENTS.md`              Scope, personas, requirements,
                                                       features and acceptance criteria

  `02-ARCHITECTURE-AND-DATA-FLOW.md`                   Components, topology, data flow,
                                                       cloud responsibilities and
                                                       resilience

  `03-TECH-STACK-AND-INFRASTRUCTURE.md`                Technology choices, repository
                                                       layout, environments, Docker and
                                                       Terraform

  `04-DATA-CONTRACTS-AND-TRANSFORMATIONS.md`           Event schemas, storage layout,
                                                       warehouse model and data quality

  `05-IMPLEMENTATION-PLAN-AND-AGENT-INSTRUCTIONS.md`   A-to-Z build sequence, coding
                                                       rules, tests and definition of done

  `06-OPERATIONS-SECURITY-AND-COST.md`                 Security, observability, recovery,
                                                       cost controls and runbooks

  `07-PORTFOLIO-AND-DEMO.md`                           Dashboard, demo script, screenshots
                                                       and resume-ready outcomes
  ----------------------------------------------------------------------------------------

## Recommended implementation approach

Start with a local end-to-end vertical slice using Docker Compose. Then
add AWS and GCP integrations behind clear interfaces. This avoids paying
for cloud resources before the pipeline works locally.

**Cloud roles (initial design):** - **AWS:** S3 object storage for the
primary raw-data landing zone; AWS Glue Data Catalog for cataloguing the
AWS copy. - **Google Cloud:** Cloud Storage for a replicated raw-data
copy; BigQuery as the analytical warehouse. - **Portable processing:**
Kafka, PySpark, Airflow and dbt run in containers initially. Do not
imply that a locally hosted service is a managed cloud service.

The first release is a portfolio implementation, not a
payment-processing system. Generate synthetic orders and payment events
only. Never collect real card details or production customer
information.

## Non-negotiable engineering principles

1.  Make every pipeline stage restartable and idempotent.
2.  Preserve immutable raw events; transformations create new datasets.
3.  Include event IDs, schema versions, event timestamps and ingestion
    timestamps.
4.  Handle duplicate, late, malformed and out-of-order events
    explicitly.
5.  Keep secrets out of source control and logs.
6.  Make local development possible without cloud credentials.
7.  Make cloud deployment opt-in and document expected costs.
8.  Include tests, sample data, architecture diagrams and reproducible
    setup instructions.
9.  Never claim cross-cloud failover, exactly-once delivery or disaster
    recovery unless the implementation and tests demonstrate it.
10. Prefer a working, documented vertical slice over placeholder files
    or excessive abstraction.
