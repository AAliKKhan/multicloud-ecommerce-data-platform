# ADR 0008: One dbt project for DuckDB and BigQuery

- Status: accepted
- Date: 2026-11-10

## Context

BigQuery cannot run locally and no GCP account is available, yet the models
must be built, tested and trusted.

## Decision

- A single dbt project targets both DuckDB (local stand-in) and BigQuery.
  Dialect differences (JSON extraction, timestamp parsing, array explosion,
  numeric types) are isolated in `dbt/macros/cross_db.sql` using
  `adapter.dispatch`. Models contain no dialect-specific SQL.
- Conservative SQL only: no `QUALIFY`, no dynamic JSON paths, no
  engine-specific aggregates; `row_number()` in a CTE instead.
- No external dbt packages, so `dbt deps` and network access are not needed.
- **Verification layers:** (1) `dbt build` on DuckDB (85+ tests, singular
  reconciliation tests); (2) an end-to-end test that recomputes sales, product,
  payment and inventory metrics independently in Python and compares them with
  the marts; (3) an offline `dbt compile` against a compile-only BigQuery target
  using a throwaway generated key, with the output parsed as BigQuery SQL by
  sqlglot. Layer 3 proves syntax only. **Semantics on BigQuery stay unverified**
  until a real project is available.
- Referential-integrity tests tied to quarantined parent events are warnings,
  not failures, so data-quality findings are surfaced without blocking the
  pipeline; key uniqueness and metric reconciliation tests are hard failures.
- Cost guard: the BigQuery profile sets `maximum_bytes_billed` (1 GB per query).

## Consequences

Model SQL is limited to the intersection of DuckDB and BigQuery features. If a
model needs a dialect feature, add a dispatching macro rather than branching in
the model.
