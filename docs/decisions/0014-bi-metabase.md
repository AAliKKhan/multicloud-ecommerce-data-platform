# ADR 0014: Business dashboard with Metabase

- Status: accepted
- Date: 2026-11-11

## Context

The specification asks for a Metabase or Looker Studio dashboard over the curated marts. Until now the only
business view was a static HTML file. The local warehouse is DuckDB (a stand-in for BigQuery), the machine has
about 5 GB for containers, and no cloud account exists.

## Decision

- **Metabase, pinned (`v0.65.3`), opt-in under the `bi` profile**, with its own Postgres for its application
  state. Looker Studio was rejected: it is a hosted Google service that needs a cloud account and cannot be
  run or tested locally.
- **Publish the marts to Postgres instead of connecting Metabase to DuckDB.** The community DuckDB driver
  exists and matches our DuckDB version, but DuckDB allows either one writer or many read-only openers of a
  file. A BI tool holding the warehouse open blocks the pipeline's loads, and an embedded engine inside the
  Metabase JVM would add a second DuckDB runtime to keep in step. A copy decouples them. Trade-off recorded: the
  dashboard is as fresh as the last `ecomm-bi publish`; the freshness table on the dashboard shows when that was.
- **The publish is one transaction** (drop, create, copy, verify row counts, commit), only for the four marts
  and a freshness row, never raw or intermediate tables. A missing mart fails before Postgres is touched. An
  unknown column type is an error, never a guess.
- **Dashboard as code.** Questions are native SQL in a Python spec, provisioned through Metabase's REST API,
  idempotent by name. Metric definitions stay in dbt; the SQL only selects, sums over days, or ranks. Metabase's
  UI-built state is not the source of truth, so the dashboard can be rebuilt from scratch.
- **Least privilege.** `metabase`, `bi_publisher` and `bi_reader` roles; Metabase connects as the read-only
  `bi_reader`. The Postgres superuser password is only in the Postgres container's environment. All passwords are
  random, generated locally and git-ignored; services refuse to start without them.
- **Verification compares numbers, not just status codes.** The integration test runs every question through
  Metabase and compares the results with the DuckDB marts, which are the source of truth.

## Consequences

- A defect found while building: Metabase rejected the first generated admin password as "too common". The
  generator now draws until the password has lower case, upper case, digits and a symbol (unit-tested).
- A second defect, found by looking at the rendered dashboard: the synthetic-data notice was clipped to two
  lines by an inner scrollbar. The notice card is now three rows high. Automated tests would not have caught
  this; visual checks remain manual.
- A Postgres copy of the marts is another place where data lives. It holds only synthetic aggregates and is
  rebuilt on every publish.
- In the cloud, Metabase would connect to BigQuery directly with a read-only service account and the publish
  step would be dropped. That path is unverified.
