"""End-to-end (Docker-free): generate -> ingest -> dual raw landing -> parity -> warehouse -> dbt.

The business metrics produced by dbt are checked against an INDEPENDENT
re-computation in plain Python from the generated events, so the SQL is not
merely compared with itself. Replaying the loads and rebuilding must leave every
total unchanged (acceptance criterion: re-running a batch never double-counts).

Local substitutes in play: directories for S3/GCS, DuckDB for BigQuery.
"""

from __future__ import annotations

import shutil
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pytest

from dbt_support import copy_project, run_dbt
from ecomm.contracts import ContractViolation, EventType, parse_event
from ecomm.contracts import payloads as p
from ecomm.demo import SOURCE, run_batch_demo
from ecomm.generator import GeneratorConfig, generate
from ecomm.observability.cli import snapshot_for_demo_dir
from ecomm.quality import CheckStatus, QualityContext, persist_report, run_quality_suite
from ecomm.report.dashboard import build_dashboard
from ecomm.storage.local import LocalObjectStore
from ecomm.warehouse.client import utcnow
from ecomm.warehouse.duckdb_client import DuckDBClient
from ecomm.warehouse.loader import WarehouseLoader

pytestmark = pytest.mark.dbt

START = GeneratorConfig().start
CONFIG = GeneratorConfig(
    seed=21,
    orders=250,
    customers=40,
    end=START + timedelta(days=3),
    malformed_rate=0.03,
    late_rate=0.03,
    out_of_order_rate=0.06,
    duplicate_rate=0.04,
)


# --- independent oracle --------------------------------------------------------------------


def expected_metrics(config: GeneratorConfig) -> dict[str, Any]:
    seen: set[str] = set()
    parsed = []
    for emission in generate(config).emissions:
        try:
            event = parse_event(emission.value)
        except ContractViolation:
            continue
        if str(event.envelope.event_id) in seen:
            continue
        seen.add(str(event.envelope.event_id))
        parsed.append(event)

    created: dict[str, p.OrderCreated] = {}
    state: dict[str, tuple[int, Any, str, str]] = {}  # order -> (version, time, event_id, status)
    captured: dict[str, Decimal] = defaultdict(Decimal)
    refunded: dict[str, Decimal] = defaultdict(Decimal)
    terminal: dict[str, tuple[Any, str]] = {}
    inventory: dict[tuple[str, str], int] = defaultdict(int)

    for event in parsed:
        env, payload = event.envelope, event.payload
        et = env.event_type
        if isinstance(payload, p.OrderCreated):
            created[payload.order_id] = payload
            candidate = (payload.order_version, env.event_time, str(env.event_id), "created")
        elif isinstance(payload, p.OrderUpdated):
            candidate = (
                payload.order_version,
                env.event_time,
                str(env.event_id),
                payload.order_status.value,
            )
        elif isinstance(payload, p.OrderCancelled):
            candidate = (payload.order_version, env.event_time, str(env.event_id), "cancelled")
        else:
            candidate = None
        if candidate is not None:
            oid = payload.order_id  # type: ignore[union-attr]
            if oid not in state or candidate[:3] > state[oid][:3]:
                state[oid] = candidate
        if isinstance(payload, p.PaymentBase | p.PaymentFailed):
            if et is EventType.PAYMENT_CAPTURED:
                captured[payload.order_id] += payload.amount
            elif et is EventType.PAYMENT_REFUNDED:
                refunded[payload.order_id] += payload.amount
            if et in (EventType.PAYMENT_CAPTURED, EventType.PAYMENT_FAILED):
                current = terminal.get(payload.payment_id)
                outcome = (
                    payload.payment_time,
                    "captured" if et is EventType.PAYMENT_CAPTURED else "failed",
                )
                if current is None or outcome[0] < current[0]:
                    terminal[payload.payment_id] = outcome
        if isinstance(payload, p.InventoryBase):
            inventory[(payload.product_id, payload.location_id)] += payload.quantity_delta

    daily: dict[date, dict[str, Any]] = defaultdict(
        lambda: {"gmv": Decimal(0), "net": Decimal(0), "orders": 0, "placed": 0, "cancelled": 0}
    )
    units: dict[str, int] = defaultdict(int)
    for oid, order in created.items():
        status = state[oid][3]
        day = daily[order.order_time.date()]
        day["placed"] += 1
        if status == "cancelled":
            day["cancelled"] += 1
            continue
        day["gmv"] += order.subtotal
        if captured[oid] > 0:
            day["orders"] += 1
            day["net"] += order.subtotal - order.discount_amount - refunded[oid]
            for item in order.items:
                units[item.product_id] += item.quantity

    health: dict[date, dict[str, int]] = defaultdict(lambda: {"ok": 0, "failed": 0})
    for when, outcome in terminal.values():
        health[when.date()]["ok" if outcome == "captured" else "failed"] += 1

    return {
        "daily": dict(daily),
        "units": dict(units),
        "health": dict(health),
        "inventory": dict(inventory),
    }


# --- pipeline fixture ------------------------------------------------------------------------


@pytest.fixture(scope="module")
def pipeline(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    root = tmp_path_factory.mktemp("e2e")
    summary = run_batch_demo(CONFIG, root / "demo", batches=3)
    project = copy_project(root)
    shutil.copy(root / "demo" / "products.csv", project / "seeds" / "products.csv")
    env = {"DBT_DUCKDB_PATH": str(root / "demo" / "warehouse.duckdb")}
    build = run_dbt(project, "build", "--target", "duckdb", env=env)
    return {"summary": summary, "project": project, "env": env, "build": build, "root": root}


def query(pipeline: dict[str, Any], sql: str) -> list[tuple[Any, ...]]:
    con = duckdb.connect(pipeline["env"]["DBT_DUCKDB_PATH"], read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


# --- tests ---------------------------------------------------------------------------------------


def test_pipeline_stages_succeeded(pipeline: dict[str, Any]) -> None:
    summary = pipeline["summary"]
    assert summary["parity_ok"] and summary["warehouse_load_ok"]
    assert summary["quarantined"] > 0 and summary["duplicates_dropped"] > 0  # faults were exercised
    assert summary["warehouse_rows_inserted"] == summary["valid_events"]
    assert all(b["status"] == "COMPLETE" for b in summary["batches"])


def test_dbt_build_has_no_errors(pipeline: dict[str, Any]) -> None:
    build = pipeline["build"]
    assert build.returncode == 0, build.stdout[-4000:]
    assert " ERROR=0 " in build.stdout
    assert "FAIL" not in build.stdout.replace("FAIL=0", "")


def test_daily_sales_match_independent_computation(pipeline: dict[str, Any]) -> None:
    expected = expected_metrics(CONFIG)["daily"]
    rows = query(
        pipeline,
        "SELECT order_date, orders_placed, order_count, cancelled_orders, "
        "gross_merchandise_value, net_sales FROM analytics.mart_daily_sales ORDER BY 1",
    )
    assert {r[0] for r in rows} == set(expected)
    for order_date, placed, orders, cancelled, gmv, net in rows:
        want = expected[order_date]
        assert (placed, orders, cancelled) == (want["placed"], want["orders"], want["cancelled"])
        assert gmv == want["gmv"]
        assert net == want["net"]
    assert sum(r[5] for r in rows) > 0


def test_average_order_value_is_net_sales_over_qualifying_orders(pipeline: dict[str, Any]) -> None:
    for _, orders, net, aov in query(
        pipeline,
        "SELECT order_date, order_count, net_sales, average_order_value "
        "FROM analytics.mart_daily_sales",
    ):
        assert float(aov) == pytest.approx(float(round(net / orders, 2)), abs=0.005)


def test_product_units_match_independent_computation(pipeline: dict[str, Any]) -> None:
    expected = expected_metrics(CONFIG)["units"]
    rows = dict(
        query(pipeline, "SELECT product_id, units_sold FROM analytics.mart_product_performance")
    )
    assert {k: int(v) for k, v in rows.items()} == expected


def test_payment_health_matches_independent_computation(pipeline: dict[str, Any]) -> None:
    expected = expected_metrics(CONFIG)["health"]
    rows = query(
        pipeline,
        "SELECT payment_date, successful_payments, failed_payments, success_rate "
        "FROM analytics.mart_payment_health",
    )
    assert {r[0] for r in rows} == set(expected)
    for day, ok, failed, rate in rows:
        assert (ok, failed) == (expected[day]["ok"], expected[day]["failed"])
        assert float(rate) == pytest.approx(round(ok / (ok + failed), 4))


def test_inventory_on_hand_matches_independent_computation(pipeline: dict[str, Any]) -> None:
    expected = expected_metrics(CONFIG)["inventory"]
    rows = {
        (r[0], r[1]): int(r[2])
        for r in query(
            pipeline,
            "SELECT product_id, location_id, units_on_hand FROM analytics.mart_inventory_risk",
        )
    }
    assert rows == expected


def test_replaying_loads_and_rebuilding_changes_nothing(pipeline: dict[str, Any]) -> None:
    before = query(
        pipeline,
        "SELECT SUM(net_sales), SUM(gross_merchandise_value), SUM(order_count) "
        "FROM analytics.mart_daily_sales",
    )
    raw_before = query(pipeline, "SELECT COUNT(*) FROM raw.raw_events")

    root: Path = pipeline["root"] / "demo"
    client = DuckDBClient(root / "warehouse.duckdb")
    try:
        loader = WarehouseLoader(client, LocalObjectStore(root / "lake" / "s3-local"), SOURCE)
        report = loader.backfill(CONFIG.start.date(), CONFIG.end.date(), "replay-1")
    finally:
        client.close()
    assert report.ok
    assert sum(r.rows_inserted for r in report.results) == 0
    assert sum(r.rows_duplicate for r in report.results) == raw_before[0][0]

    rebuild = run_dbt(pipeline["project"], "build", "--target", "duckdb", env=pipeline["env"])
    assert rebuild.returncode == 0, rebuild.stdout[-3000:]
    assert query(pipeline, "SELECT COUNT(*) FROM raw.raw_events") == raw_before
    assert (
        query(
            pipeline,
            "SELECT SUM(net_sales), SUM(gross_merchandise_value), SUM(order_count) "
            "FROM analytics.mart_daily_sales",
        )
        == before
    )


# --- quality, metrics and dashboard on the REAL dbt output ----------------------------------


def test_quality_suite_passes_on_the_real_marts(pipeline: dict[str, Any]) -> None:
    root: Path = pipeline["root"] / "demo"
    s3 = LocalObjectStore(root / "lake" / "s3-local", name="s3-local")
    gcs = LocalObjectStore(root / "lake" / "gcs-local", name="gcs-local")
    control = LocalObjectStore(root / "lake" / "control", name="control")
    client = DuckDBClient(root / "warehouse.duckdb")
    try:
        ctx = QualityContext(
            client=client,
            source=SOURCE,
            now=utcnow(),
            primary_store=s3,
            lake_stores={"s3-local": s3, "gcs-local": gcs},
            control_store=control,
        )
        report = run_quality_suite(ctx)
        persist_report(report, client, control, SOURCE)
    finally:
        client.close()
    assert report.ok, report.format_text()
    assert report.count(CheckStatus.SKIP) == 0
    for check_id in (
        "raw.event_id_unique",
        "marts.amounts_in_bounds",
        "marts.payments_reconcile_to_orders",
        "marts.daily_sales_reconcile_to_orders",
        "reconcile.warehouse_row_count",
        "parity.cross_cloud",
    ):
        assert report.get(check_id).status is CheckStatus.PASS, check_id


def test_business_metrics_are_exported_and_match_the_oracle(pipeline: dict[str, Any]) -> None:
    expected = expected_metrics(CONFIG)["daily"]
    snap = snapshot_for_demo_dir(pipeline["root"] / "demo")
    assert snap.business_available
    assert snap.business_orders == sum(d["orders"] for d in expected.values())
    assert snap.business_net_sales == pytest.approx(float(sum(d["net"] for d in expected.values())))
    assert snap.quality_checks["FAIL"] == 0 and snap.quality_last_run_ts is not None


def test_dashboard_shows_the_oracle_figures(pipeline: dict[str, Any]) -> None:
    expected = expected_metrics(CONFIG)
    net = sum(d["net"] for d in expected["daily"].values())
    orders = sum(d["orders"] for d in expected["daily"].values())
    root: Path = pipeline["root"] / "demo"
    client = DuckDBClient(root / "warehouse.duckdb", read_only=True)
    try:
        out = build_dashboard(
            client, root / "dashboard.html", utcnow(), LocalObjectStore(root / "lake" / "control")
        )
    finally:
        client.close()
    html = out.read_text(encoding="utf-8")
    assert f"${net:,.2f}" in html
    assert f">{orders:,}<" in html
    assert "synthetic" in html.lower() and "<script" not in html
    assert "Latest quality run" in html  # the persisted quality report is surfaced
