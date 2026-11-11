"""Pipeline metrics for Prometheus.

Pipeline/infrastructure health (``ecomm_*``) is kept apart from business
totals (``ecomm_business_*``). Labels are low-cardinality on purpose (status,
event_type, section): never an order, customer or payment ID.

Every section is collected independently; if one fails (e.g. the marts have not
been built yet) the others still report and ``ecomm_scrape_errors{section}``
is set to 1 for the broken one.

Verified here only through Python tests. Prometheus and Grafana themselves
have not been run (they need Docker).
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

from prometheus_client import CollectorRegistry
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.registry import Collector

from ecomm.lake.manifest import BatchManifest, manifest_key
from ecomm.lake.parity import list_batch_ids, reconcile_all, reconcile_all_light
from ecomm.lake.stats import list_ingest_stats
from ecomm.quality.checks import QualityContext
from ecomm.quality.models import CheckStatus
from ecomm.storage.interface import StorageError
from ecomm.warehouse.client import WarehouseError
from ecomm.warehouse.schema import LOAD_AUDIT, RAW_EVENTS

BATCH_STATUSES = ("COMPLETE", "PARTIAL", "FAILED")
LOAD_STATUSES = ("SUCCEEDED", "FAILED")
SECTIONS = ("lake", "warehouse", "ingest", "spark", "parity", "quality", "business")


@dataclass
class Snapshot:
    now: datetime
    batches: dict[str, int] = field(default_factory=dict)
    events_committed: int = 0
    events_by_type: dict[str, int] = field(default_factory=dict)
    load_attempts: dict[str, int] = field(default_factory=dict)
    load_rows_inserted: int = 0
    load_rows_duplicate: int = 0
    last_load_ts: float | None = None
    ingest_input: int = 0
    ingest_valid: int = 0
    ingest_duplicates: int = 0
    ingest_quarantined: int = 0
    ingest_late: int = 0
    # Spark keeps its own offsets (no Kafka consumer group), so consumer lag is derived:
    # kafka end offset (from the Kafka exporter) minus this "next offset to read".
    spark_next_offsets: dict[tuple[str, int], int] = field(default_factory=dict)
    spark_last_batch_ts: float | None = None
    parity_checked: int = 0
    parity_mismatched: int = 0
    quality_checks: dict[str, int] = field(default_factory=dict)
    quality_last_run_ts: float | None = None
    business_available: bool = False
    business_orders: int = 0
    business_net_sales: float = 0.0
    errors: dict[str, int] = field(default_factory=dict)


def _ts(value: datetime) -> float:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).timestamp()


def collect_snapshot(ctx: QualityContext, parity: str = "full") -> Snapshot:
    """Collect every metric section.

    ``parity="full"`` re-reads and hashes every raw object in both destinations (right for a
    one-shot export or a daily job); ``"light"`` compares manifests and object presence only,
    which is cheap enough to run every minute against a lake of thousands of files.
    """
    snap = Snapshot(now=ctx.now, errors=dict.fromkeys(SECTIONS, 0))
    snap.batches = dict.fromkeys(BATCH_STATUSES, 0)
    snap.load_attempts = dict.fromkeys(LOAD_STATUSES, 0)
    snap.quality_checks = {s.value: 0 for s in CheckStatus}
    client = ctx.client

    try:  # lake: ledger + manifests
        if ctx.control_store is not None:
            for key in ctx.control_store.list_objects(f"ledger/source={ctx.source}/"):
                status = json.loads(ctx.control_store.read_object(key))["status"]
                snap.batches[status] = snap.batches.get(status, 0) + 1
        if ctx.primary_store is not None:
            for batch_id in list_batch_ids(ctx.primary_store, ctx.source):
                raw = ctx.primary_store.read_object(manifest_key(ctx.source, batch_id))
                snap.events_committed += BatchManifest.from_bytes(raw).event_count
    except (StorageError, ValueError, KeyError):
        snap.errors["lake"] = 1

    try:  # warehouse: ledger + audit
        for row in client.query(
            f"SELECT event_type, COUNT(*) AS n FROM {client.ref(RAW_EVENTS.name)} "
            "GROUP BY event_type"
        ):
            snap.events_by_type[str(row["event_type"])] = int(row["n"])
        for row in client.query(
            f"SELECT status, COUNT(*) AS n, SUM(rows_inserted) AS ins, SUM(rows_duplicate) AS dup "
            f"FROM {client.ref(LOAD_AUDIT.name)} GROUP BY status"
        ):
            snap.load_attempts[str(row["status"])] = int(row["n"])
            snap.load_rows_inserted += int(row["ins"] or 0)
            snap.load_rows_duplicate += int(row["dup"] or 0)
        last = client.query(
            f"SELECT MAX(finished_at) AS t FROM {client.ref(LOAD_AUDIT.name)} "
            "WHERE status = 'SUCCEEDED'"
        )[0]["t"]
        snap.last_load_ts = _ts(last) if last is not None else None
    except WarehouseError:
        snap.errors["warehouse"] = 1

    try:  # ingest statistics
        if ctx.control_store is not None:
            for stats in list_ingest_stats(ctx.control_store, ctx.source):
                snap.ingest_input += stats.input_count
                snap.ingest_valid += stats.valid
                snap.ingest_duplicates += stats.duplicates
                snap.ingest_quarantined += stats.quarantined
                snap.ingest_late += stats.late
    except (StorageError, ValueError, KeyError):
        snap.errors["ingest"] = 1

    try:  # Spark progress: offsets processed per topic partition
        if ctx.control_store is not None:
            for key in ctx.control_store.list_objects(f"spark/source={ctx.source}/"):
                progress = json.loads(ctx.control_store.read_object(key))
                for tp, (_, high) in progress["offsets"].items():
                    topic, _, partition = tp.rpartition("/")
                    slot = (topic, int(partition))
                    snap.spark_next_offsets[slot] = max(
                        snap.spark_next_offsets.get(slot, 0), high + 1
                    )
                recorded = datetime.fromisoformat(progress["recorded_at"]).timestamp()
                snap.spark_last_batch_ts = max(snap.spark_last_batch_ts or 0.0, recorded)
    except (StorageError, ValueError, KeyError):
        snap.errors["spark"] = 1

    try:  # cross-cloud parity
        if len(ctx.lake_stores) >= 2:
            check = reconcile_all_light if parity == "light" else reconcile_all
            reports = check(ctx.lake_stores, ctx.source)
            snap.parity_checked = len(reports)
            snap.parity_mismatched = sum(1 for r in reports if not r.ok)
    except StorageError:
        snap.errors["parity"] = 1

    try:  # most recent persisted quality report
        if ctx.control_store is not None:
            keys = ctx.control_store.list_objects(f"quality/source={ctx.source}/")
            if keys:
                latest = json.loads(ctx.control_store.read_object(keys[-1]))
                for status, count in latest["counts"].items():
                    snap.quality_checks[status] = int(count)
                snap.quality_last_run_ts = datetime.fromisoformat(latest["checked_at"]).timestamp()
    except (StorageError, ValueError, KeyError):
        snap.errors["quality"] = 1

    try:  # business totals (only after dbt has built the marts)
        row = client.query(
            f"SELECT COUNT(CASE WHEN is_qualifying THEN 1 END) AS orders, "
            f"COALESCE(SUM(net_sales_amount), 0) AS net FROM {client.ref_analytics('fct_orders')}"
        )[0]
        snap.business_orders = int(row["orders"])
        snap.business_net_sales = float(row["net"])
        snap.business_available = True
    except WarehouseError:
        snap.business_available = False  # not an error: marts may not exist yet

    return snap


def _gauge(
    name: str,
    doc: str,
    value: float,
    labels: Mapping[str, float] | None = None,
    label: str = "",
) -> Metric:
    if labels is None:
        family = GaugeMetricFamily(name, doc, value=value)
    else:
        family = GaugeMetricFamily(name, doc, labels=[label])
        for key, val in sorted(labels.items()):
            family.add_metric([key], val)
    return family


def snapshot_to_families(s: Snapshot) -> list[Metric]:
    since_load = (
        (s.now.timestamp() - s.last_load_ts) if s.last_load_ts is not None else float("inf")
    )
    rate = s.ingest_quarantined / s.ingest_input if s.ingest_input else 0.0
    families = [
        _gauge("ecomm_batches", "Raw batches by commit status.", 0, s.batches, "status"),
        _gauge("ecomm_events_committed", "Events in committed raw batches.", s.events_committed),
        _gauge(
            "ecomm_warehouse_events",
            "Events in the warehouse ledger by type.",
            0,
            s.events_by_type,
            "event_type",
        ),
        _gauge(
            "ecomm_load_attempts",
            "Warehouse load attempts by status.",
            0,
            s.load_attempts,
            "status",
        ),
        _gauge("ecomm_load_rows_inserted", "Rows inserted by loads.", s.load_rows_inserted),
        _gauge(
            "ecomm_load_rows_duplicate",
            "Rows skipped by loads as already present.",
            s.load_rows_duplicate,
        ),
        _gauge(
            "ecomm_last_successful_load_timestamp_seconds",
            "Unix time of the last successful warehouse load (0 if never).",
            s.last_load_ts or 0.0,
        ),
        _gauge(
            "ecomm_seconds_since_last_successful_load",
            "Seconds since the last successful load (+Inf if never).",
            since_load,
        ),
        _gauge("ecomm_ingest_input_messages", "Messages seen by ingest.", s.ingest_input),
        _gauge("ecomm_events_valid", "Valid events accepted by ingest.", s.ingest_valid),
        _gauge(
            "ecomm_events_duplicates", "Duplicate events dropped by ingest.", s.ingest_duplicates
        ),
        _gauge("ecomm_events_quarantined", "Events quarantined by ingest.", s.ingest_quarantined),
        _gauge("ecomm_events_late", "Late events kept and flagged.", s.ingest_late),
        _gauge("ecomm_quarantine_rate", "Quarantined / input messages.", rate),
        _gauge("ecomm_parity_batches_checked", "Batches compared across clouds.", s.parity_checked),
        _gauge(
            "ecomm_parity_batches_mismatched", "Batches whose clouds disagree.", s.parity_mismatched
        ),
        _gauge(
            "ecomm_quality_checks",
            "Latest quality run, checks by status.",
            0,
            s.quality_checks,
            "status",
        ),
        _gauge(
            "ecomm_quality_last_run_timestamp_seconds",
            "Unix time of the last persisted quality run (0 if none).",
            s.quality_last_run_ts or 0.0,
        ),
        _gauge(
            "ecomm_business_metrics_available",
            "1 if the marts could be read.",
            1.0 if s.business_available else 0.0,
        ),
        _gauge(
            "ecomm_business_qualifying_orders",
            "BUSINESS: qualifying orders (synthetic).",
            s.business_orders,
        ),
        _gauge(
            "ecomm_business_net_sales_usd",
            "BUSINESS: net sales in USD (synthetic).",
            s.business_net_sales,
        ),
        _gauge(
            "ecomm_scrape_errors",
            "1 when a metrics section failed to collect.",
            0,
            {k: float(v) for k, v in s.errors.items()},
            "section",
        ),
        _gauge(
            "ecomm_scrape_timestamp_seconds", "Unix time of this collection.", s.now.timestamp()
        ),
    ]
    offsets = GaugeMetricFamily(
        "ecomm_spark_next_offset",
        "Next Kafka offset the Spark job will read, per topic partition (lag = Kafka end - this).",
        labels=["topic", "partition"],
    )
    for (topic, partition), next_offset in sorted(s.spark_next_offsets.items()):
        offsets.add_metric([topic, str(partition)], next_offset)
    families.append(offsets)
    families.append(
        _gauge(
            "ecomm_spark_last_batch_timestamp_seconds",
            "Unix time the Spark job last recorded a micro-batch (0 if never).",
            s.spark_last_batch_ts or 0.0,
        )
    )
    return families


class _SnapshotCollector(Collector):
    def __init__(self, snapshot: Snapshot) -> None:
        self._snapshot = snapshot

    def collect(self) -> Iterable[Metric]:
        return snapshot_to_families(self._snapshot)


def build_registry(snapshot: Snapshot) -> CollectorRegistry:
    registry = CollectorRegistry()
    registry.register(_SnapshotCollector(snapshot))
    return registry


def metric_names(snapshot: Snapshot) -> set[str]:
    """Sample names exposed for a snapshot (what Prometheus queries can reference)."""
    return {sample.name for family in snapshot_to_families(snapshot) for sample in family.samples}
