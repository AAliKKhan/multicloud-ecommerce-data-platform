# ADR 0013: Monitoring stack

- Status: accepted
- Date: 2026-11-11

## Context

Metrics, alert rules and a dashboard JSON existed since the observability step but had never been run in
Prometheus or Grafana. The machine has about 5 GB for containers, and the lake lives on a slow Windows bind mount.

## Decision

- **Pinned images, opt-in profile.** `prom/prometheus:v3.5.0`, `grafana/grafana:11.6.0`,
  `danielqsj/kafka-exporter:v1.9.0`, and a small project image for the pipeline exporter, all under the
  `monitoring` profile (about 0.8 GB). Prometheus and Grafana publish loopback-only ports; the exporters publish
  none.
- **Grafana fails closed.** An entrypoint wrapper refuses to start without a generated random admin password
  (`docker/monitoring/generate_env.py`, git-ignored, owner-only where supported) instead of using the default
  login. Anonymous access, sign-up, analytics and update checks are off; the dashboard and datasource are
  provisioned read-only from files.
- **The exporter never computes on the request path.** A background thread refreshes a snapshot; `collect()` serves
  it from memory and exports its age and refresh duration. Reason: measured, not assumed. Computing on every
  scrape took over two minutes here (thousands of tiny files through a bind mount, full byte-level parity), so
  Prometheus timed out, the container health check failed and the exporter looked down while healthy.
- **Two parity levels.** The exporter's default is *light* (manifests and object presence). *Full* byte-level
  verification belongs to the daily reconciliation DAG and the one-shot export. Trade-off recorded: the light check
  cannot detect an object altered in place.
- **Degraded, not dead.** A missing, corrupt or locked warehouse produces `ecomm_exporter_ready 0` with the
  exception class as the reason; the exporter keeps serving its previous snapshot.
- **Spark lag is derived, not scraped.** Spark keeps its own offsets (no consumer group), so the Kafka exporter's
  group lag is empty by design (`--group.filter=^$`). The pipeline exporter publishes Spark's next offset per
  partition from the per-batch progress records, and lag is `kafka end offset - next offset` in PromQL (alert and
  dashboard).
- **Alerts are tested by the real engine.** `alerts_test.yml` is executed with `promtool test rules`; a test
  checks that every alert is shown to fire in one scenario and to stay quiet in another. The Python-side
  evaluator remains only for the simple expression shapes and returns "unknown" for the rest instead of guessing.
- **Prometheus is not hot-reloaded.** The lifecycle API stays at its secure default (disabled); rule changes need a
  restart. The monitoring test compares the number of loaded rules with the file, so a stale instance is caught.

## Consequences

- A found defect worth remembering: the compose file once passed `--web.enable-lifecycle=false`, which Prometheus
  rejects (boolean flags take no value) and crash-looped. Static tests now forbid valued boolean flags.
- Spark driver metrics and Airflow metrics are not scraped; both need further components (see docs/monitoring.md).
- Capacity and throughput are unmeasured; the figures in the docs are observations from one 8 GB machine.
