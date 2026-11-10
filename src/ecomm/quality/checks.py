"""The data-quality suite (spec 4.9).

Each check returns one :class:`CheckResult`. A check that cannot be evaluated
(missing table, unreachable store) becomes a ``FAIL`` with the reason, never an
unhandled crash, so a broken pipeline is surfaced rather than hidden. Checks
that need an optional input (lake stores, control store) are ``SKIP`` when it
is absent.

Hard failures are reserved for broken keys, broken reconciliation, stale loads
and failed/partial batches. Orphans caused by quarantined parent events and
high quarantine rates are warnings first, so they are visible but expected.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from ecomm.lake.manifest import BatchManifest, manifest_key
from ecomm.lake.parity import ParityStatus, list_batch_ids, reconcile_all
from ecomm.lake.raw import event_ids_in_object
from ecomm.lake.stats import list_ingest_stats
from ecomm.quality.models import CheckResult, CheckStatus
from ecomm.storage.interface import ObjectStore, StorageError
from ecomm.warehouse.client import WarehouseClient, WarehouseError, utcnow
from ecomm.warehouse.schema import LOAD_AUDIT, RAW_EVENTS

ACCEPTED_CURRENCIES = ("USD", "EUR", "GBP")
MIN_PLAUSIBLE_EVENT_TIME = datetime(2020, 1, 1, tzinfo=UTC)


@dataclass
class QualityContext:
    client: WarehouseClient
    source: str = "commerce"
    now: datetime = field(default_factory=utcnow)
    primary_store: ObjectStore | None = None  # the raw store the loader reads
    lake_stores: Mapping[str, ObjectStore] = field(default_factory=dict)  # all clouds
    control_store: ObjectStore | None = None  # ledger + ingest stats
    freshness_sla: timedelta = timedelta(hours=24)
    quarantine_warn_rate: float = 0.05
    quarantine_fail_rate: float = 0.25


Check = Callable[[QualityContext], CheckResult]
_REGISTRY: list[tuple[str, Check]] = []


def check(check_id: str) -> Callable[[Check], Check]:
    def register(fn: Check) -> Check:
        _REGISTRY.append((check_id, fn))
        return fn

    return register


# --- helpers -------------------------------------------------------------------------------------


def _count(ctx: QualityContext, sql: str) -> int:
    value = ctx.client.query(sql)[0]["n"]
    return int(value or 0)


def _ts(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def _zero(check_id: str, observed: int, what: str, *, warn: bool = False) -> CheckResult:
    if observed == 0:
        return CheckResult(check_id, CheckStatus.PASS, "0", "0")
    status = CheckStatus.WARN if warn else CheckStatus.FAIL
    return CheckResult(check_id, status, str(observed), "0", what)


def _lit(moment: datetime) -> str:
    return f"TIMESTAMP '{moment.astimezone(UTC):%Y-%m-%d %H:%M:%S}'"


def _skip(check_id: str, why: str) -> CheckResult:
    return CheckResult(check_id, CheckStatus.SKIP, "-", "-", why)


# --- raw ledger -----------------------------------------------------------------------------------


@check("raw.required_fields_not_null")
def required_fields(ctx: QualityContext) -> CheckResult:
    n = _count(
        ctx,
        f"SELECT COUNT(*) AS n FROM {ctx.client.ref(RAW_EVENTS.name)} WHERE event_id IS NULL "
        "OR event_type IS NULL OR event_time IS NULL OR payload IS NULL",
    )
    return _zero("raw.required_fields_not_null", n, "rows missing a required identifier/timestamp")


@check("raw.event_id_unique")
def event_id_unique(ctx: QualityContext) -> CheckResult:
    n = _count(
        ctx,
        f"SELECT COUNT(*) - COUNT(DISTINCT event_id) AS n FROM {ctx.client.ref(RAW_EVENTS.name)}",
    )
    return _zero("raw.event_id_unique", n, "duplicate event_id rows (dedup/MERGE broken)")


@check("raw.event_time_plausible")
def event_time_plausible(ctx: QualityContext) -> CheckResult:
    n = _count(
        ctx,
        f"SELECT COUNT(*) AS n FROM {ctx.client.ref(RAW_EVENTS.name)} "
        f"WHERE event_time < {_lit(MIN_PLAUSIBLE_EVENT_TIME)} "
        f"OR event_time > {_lit(ctx.now + timedelta(days=1))}",
    )
    return _zero("raw.event_time_plausible", n, "event_time outside 2020-01-01..now+1d")


# --- marts ----------------------------------------------------------------------------------------


@check("marts.amounts_in_bounds")
def amounts_in_bounds(ctx: QualityContext) -> CheckResult:
    a = ctx.client.ref_analytics
    n = _count(
        ctx,
        f"SELECT (SELECT COUNT(*) FROM {a('fct_orders')} WHERE total_amount <= 0 "
        f"OR total_amount > 100000) + (SELECT COUNT(*) FROM {a('fct_order_items')} "
        "WHERE quantity < 1 OR quantity > 100) AS n",
    )
    return _zero(
        "marts.amounts_in_bounds", n, "order totals outside (0,100000] or quantity outside 1..100"
    )


@check("marts.currency_accepted")
def currency_accepted(ctx: QualityContext) -> CheckResult:
    accepted = ", ".join(f"'{c}'" for c in ACCEPTED_CURRENCIES)
    n = _count(
        ctx,
        f"SELECT COUNT(*) AS n FROM {ctx.client.ref_analytics('fct_orders')} "
        f"WHERE currency NOT IN ({accepted})",
    )
    return _zero("marts.currency_accepted", n, f"currency not in {ACCEPTED_CURRENCIES}")


@check("marts.items_reference_orders_and_products")
def items_reference(ctx: QualityContext) -> CheckResult:
    a = ctx.client.ref_analytics
    n = _count(
        ctx,
        f"SELECT COUNT(*) AS n FROM {a('fct_order_items')} AS i "
        f"LEFT JOIN {a('fct_orders')} AS o ON o.order_id = i.order_id "
        f"LEFT JOIN {a('dim_products')} AS p ON p.product_id = i.product_id "
        "WHERE o.order_id IS NULL OR p.product_id IS NULL",
    )
    return _zero(
        "marts.items_reference_orders_and_products", n, "items with unknown order or product"
    )


@check("marts.payments_reference_orders")
def payments_reference_orders(ctx: QualityContext) -> CheckResult:
    a = ctx.client.ref_analytics
    n = _count(
        ctx,
        f"SELECT COUNT(*) AS n FROM {a('fct_payments')} AS p "
        f"LEFT JOIN {a('fct_orders')} AS o ON o.order_id = p.order_id WHERE o.order_id IS NULL",
    )
    return _zero(
        "marts.payments_reference_orders",
        n,
        "payment events whose order_created was quarantined or lost",
        warn=True,
    )


@check("marts.orders_reference_customers")
def orders_reference_customers(ctx: QualityContext) -> CheckResult:
    a = ctx.client.ref_analytics
    n = _count(
        ctx,
        f"SELECT COUNT(*) AS n FROM {a('fct_orders')} AS o "
        f"LEFT JOIN {a('dim_customers')} AS c ON c.customer_id = o.customer_id "
        "WHERE c.customer_id IS NULL",
    )
    return _zero(
        "marts.orders_reference_customers",
        n,
        "orders whose customer_created was quarantined or lost",
        warn=True,
    )


@check("marts.payments_reconcile_to_orders")
def payments_reconcile(ctx: QualityContext) -> CheckResult:
    n = _count(
        ctx,
        f"SELECT COUNT(*) AS n FROM {ctx.client.ref_analytics('fct_orders')} "
        "WHERE (captured_amount > 0 AND captured_amount <> total_amount) "
        "OR refunded_amount > captured_amount",
    )
    return _zero(
        "marts.payments_reconcile_to_orders", n, "captured != order total, or refunds > captures"
    )


@check("marts.daily_sales_reconcile_to_orders")
def daily_sales_reconcile(ctx: QualityContext) -> CheckResult:
    a = ctx.client.ref_analytics
    row = ctx.client.query(
        f"SELECT (SELECT SUM(net_sales) FROM {a('mart_daily_sales')}) AS mart, "
        f"(SELECT SUM(net_sales_amount) FROM {a('fct_orders')}) AS fact"
    )[0]
    mart, fact = row["mart"] or 0, row["fact"] or 0
    if mart == fact:
        return CheckResult(
            "marts.daily_sales_reconcile_to_orders", CheckStatus.PASS, str(mart), str(fact)
        )
    return CheckResult(
        "marts.daily_sales_reconcile_to_orders",
        CheckStatus.FAIL,
        str(mart),
        str(fact),
        "mart net sales differ from the fact table",
    )


# --- freshness ---------------------------------------------------------------------------


@check("freshness.last_successful_load")
def last_load(ctx: QualityContext) -> CheckResult:
    row = ctx.client.query(
        f"SELECT MAX(finished_at) AS t FROM {ctx.client.ref(LOAD_AUDIT.name)} "
        "WHERE status = 'SUCCEEDED'"
    )[0]
    finished = _ts(row["t"])
    expected = f"<= {ctx.freshness_sla}"
    if finished is None:
        return CheckResult(
            "freshness.last_successful_load",
            CheckStatus.FAIL,
            "never",
            expected,
            "no successful load",
        )
    age = ctx.now - finished
    status = CheckStatus.PASS if age <= ctx.freshness_sla else CheckStatus.FAIL
    detail = "" if status is CheckStatus.PASS else "warehouse load is stale (SLA breached)"
    return CheckResult("freshness.last_successful_load", status, str(age), expected, detail)


@check("freshness.latest_event")
def latest_event(ctx: QualityContext) -> CheckResult:
    row = ctx.client.query(f"SELECT MAX(event_time) AS t FROM {ctx.client.ref(RAW_EVENTS.name)}")[0]
    newest = _ts(row["t"])
    expected = f"<= {ctx.freshness_sla}"
    if newest is None:
        return CheckResult("freshness.latest_event", CheckStatus.WARN, "no events", expected)
    age = ctx.now - newest
    if age <= ctx.freshness_sla:
        return CheckResult("freshness.latest_event", CheckStatus.PASS, str(age), expected)
    return CheckResult(
        "freshness.latest_event",
        CheckStatus.WARN,
        str(age),
        expected,
        "newest event is older than the SLA",
    )


# --- source-to-target and cross-cloud -------------------------------------------------------------


def _latest_audit(ctx: QualityContext) -> dict[str, dict[str, Any]]:
    rows = ctx.client.query(
        f"SELECT batch_id, attempt, status, rows_read, expected_rows "
        f"FROM {ctx.client.ref(LOAD_AUDIT.name)}"
    )
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        if row["batch_id"] not in latest or row["attempt"] > latest[row["batch_id"]]["attempt"]:
            latest[row["batch_id"]] = row
    return latest


def _manifests(store: ObjectStore, source: str) -> list[BatchManifest]:
    return [
        BatchManifest.from_bytes(store.read_object(manifest_key(source, b)))
        for b in list_batch_ids(store, source)
    ]


@check("reconcile.loads_match_manifests")
def loads_match_manifests(ctx: QualityContext) -> CheckResult:
    cid = "reconcile.loads_match_manifests"
    if ctx.primary_store is None:
        return _skip(cid, "no primary raw store configured")
    latest = _latest_audit(ctx)
    manifests = _manifests(ctx.primary_store, ctx.source)
    unloaded = [m.batch_id for m in manifests if m.batch_id not in latest]
    failed = [
        m.batch_id
        for m in manifests
        if m.batch_id in latest and latest[m.batch_id]["status"] != "SUCCEEDED"
    ]
    mismatched = [
        m.batch_id
        for m in manifests
        if m.batch_id in latest
        and latest[m.batch_id]["status"] == "SUCCEEDED"
        and latest[m.batch_id]["rows_read"] != m.event_count
    ]
    observed = (
        f"{len(manifests)} batches: {len(unloaded)} unloaded, "
        f"{len(failed)} failed, {len(mismatched)} mismatched"
    )
    if failed or mismatched:
        return CheckResult(
            cid,
            CheckStatus.FAIL,
            observed,
            "all loaded, rows_read == manifest count",
            ",".join(failed + mismatched),
        )
    if unloaded:
        return CheckResult(cid, CheckStatus.WARN, observed, "all loaded", ",".join(unloaded))
    return CheckResult(cid, CheckStatus.PASS, observed, "all loaded, rows_read == manifest count")


@check("reconcile.warehouse_row_count")
def warehouse_row_count(ctx: QualityContext) -> CheckResult:
    cid = "reconcile.warehouse_row_count"
    if ctx.primary_store is None:
        return _skip(cid, "no primary raw store configured")
    latest = _latest_audit(ctx)
    expected_ids: set[str] = set()
    for manifest in _manifests(ctx.primary_store, ctx.source):
        if latest.get(manifest.batch_id, {}).get("status") == "SUCCEEDED":
            for entry in manifest.objects:
                expected_ids.update(event_ids_in_object(ctx.primary_store.read_object(entry.key)))
    observed = _count(ctx, f"SELECT COUNT(*) AS n FROM {ctx.client.ref(RAW_EVENTS.name)}")
    if observed == len(expected_ids):
        return CheckResult(cid, CheckStatus.PASS, str(observed), str(len(expected_ids)))
    return CheckResult(
        cid,
        CheckStatus.FAIL,
        str(observed),
        str(len(expected_ids)),
        "warehouse rows != distinct events in loaded batches",
    )


@check("parity.cross_cloud")
def cross_cloud_parity(ctx: QualityContext) -> CheckResult:
    cid = "parity.cross_cloud"
    if len(ctx.lake_stores) < 2:
        return _skip(cid, "fewer than two raw destinations configured")
    reports = reconcile_all(ctx.lake_stores, ctx.source)
    bad = [r for r in reports if r.status is not ParityStatus.MATCH]
    if bad:
        detail = "; ".join(f"{r.batch_id}: {r.problems[0]}" for r in bad[:3])
        return CheckResult(
            cid,
            CheckStatus.FAIL,
            f"{len(bad)} of {len(reports)} batches differ",
            "all batches match",
            detail,
        )
    return CheckResult(
        cid,
        CheckStatus.PASS,
        f"{len(reports)} of {len(reports)} batches match",
        "all batches match",
    )


@check("lake.batches_complete")
def batches_complete(ctx: QualityContext) -> CheckResult:
    cid = "lake.batches_complete"
    if ctx.control_store is None:
        return _skip(cid, "no control store configured")
    statuses: dict[str, str] = {}
    for key in ctx.control_store.list_objects(f"ledger/source={ctx.source}/"):
        data = json.loads(ctx.control_store.read_object(key))
        statuses[data["batch_id"]] = data["status"]
    not_complete = sorted(b for b, s in statuses.items() if s != "COMPLETE")
    if not_complete:
        return CheckResult(
            cid,
            CheckStatus.FAIL,
            f"{len(not_complete)} of {len(statuses)} not COMPLETE",
            "all COMPLETE",
            ",".join(not_complete),
        )
    return CheckResult(
        cid, CheckStatus.PASS, f"{len(statuses)} of {len(statuses)} COMPLETE", "all COMPLETE"
    )


@check("ingest.quarantine_rate")
def quarantine_rate(ctx: QualityContext) -> CheckResult:
    cid = "ingest.quarantine_rate"
    if ctx.control_store is None:
        return _skip(cid, "no control store configured")
    stats = list_ingest_stats(ctx.control_store, ctx.source)
    total = sum(s.input_count for s in stats)
    if total == 0:
        return _skip(cid, "no ingest statistics recorded")
    rate = sum(s.quarantined for s in stats) / total
    observed, expected = f"{rate:.4f}", f"<= {ctx.quarantine_warn_rate}"
    if rate > ctx.quarantine_fail_rate:
        return CheckResult(
            cid, CheckStatus.FAIL, observed, expected, "quarantine rate far above normal"
        )
    if rate > ctx.quarantine_warn_rate:
        return CheckResult(
            cid,
            CheckStatus.WARN,
            observed,
            expected,
            "high quarantine rate; inspect with ecomm-quarantine",
        )
    return CheckResult(cid, CheckStatus.PASS, observed, expected)


# --- runner ------------------------------------------------------------------------------


def registered_checks() -> list[tuple[str, Check]]:
    return list(_REGISTRY)


def run_check(check_id: str, fn: Check, ctx: QualityContext) -> CheckResult:
    try:
        return fn(ctx)
    except (WarehouseError, StorageError, ValueError, KeyError) as exc:
        return CheckResult(
            check_id,
            CheckStatus.FAIL,
            "not evaluated",
            "evaluable",
            f"{type(exc).__name__}: {str(exc)[:200]}",
        )
