# ADR 0009: Terraform scope, IAM model and what stays out

- Status: accepted
- Date: 2026-11-10

## Decision

- **Two independent stacks** (`aws`, `gcp`), each with its own providers, state,
  variables and outputs. No shared module and no cross-cloud wiring, so either
  can be created, reviewed and destroyed on its own.
- **Names, regions and projects are variables.** `environment` accepts only
  `dev` and `prod-like`; local mode needs no Terraform. Required inputs
  (`environment`, bucket names, GCP project) have no defaults, so an apply cannot
  proceed by accident.
- **No long-lived credentials anywhere.** No IAM users or access keys, no
  service-account keys. Humans and CI use SSO / ADC; workloads assume a role or
  impersonate the service account.
- **Least privilege, scoped to this project's resources:**
  - AWS pipeline policy: list/get/put on five known prefixes, **no delete**
    (overwriting needs only `PutObject`); analysts: read `raw/` and `manifests/`.
  - GCP: `storage.objectUser` on the one bucket (overwrite needs delete on the
    replaced object), `bigquery.dataEditor` on the two datasets,
    `bigquery.jobUser` on the project, read-only roles for analysts.
  - Additive IAM members only (no authoritative bindings), so existing access is
    not overwritten.
- **Secure by default:** public access blocked / enforced, ACLs disabled,
  encryption on (optional CMEK/KMS), TLS-only bucket policy on S3, versioning
  with short non-current retention, aborted incomplete uploads.
- **Catalog without a crawler:** the Glue table uses partition projection derived
  from the object-key layout (ADR 0006). It exposes the event envelope only; the
  payload is analysed in the warehouse.
- **Cost guardrails** are variables, off or minimal by default: demo expiration,
  budgets, table expiration.
- **State:** local state only for a personal sandbox; `backend.tf.example` shows
  remote state for shared use. State, plans and real `*.tfvars` are git-ignored.
- **CI never plans or applies.** It runs `fmt -check`, `init -backend=false` and
  `validate`, plus static tests that assert the security properties above.

## Out of scope

Dataflow, Kubernetes, cross-cloud replication jobs, VPC networking, and
automatic failover. Provisioning Airflow/Spark/Kafka hosts is deferred to the
Docker phase.

## Consequences

`plan`/`apply` against real accounts remain unverified until credentials and
explicit approval exist.
