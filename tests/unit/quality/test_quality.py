from __future__ import annotations

import json
import shutil
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import duckdb
import pytest

from ecomm.demo import run_batch_demo
from ecomm.generator import GeneratorConfig
from ecomm.lake.manifest import manifest_key
from ecomm.quality import CheckStatus, QualityContext, persist_report, run_quality_suite
from ecomm.quality.checks import registered_checks
from ecomm.quality.cli import main as quality_main
from ecomm.storage.local import LocalObjectStore
from ecomm.warehouse.client import utcnow
from ecomm.warehouse.duckdb_client import DuckDBClient
from support import SOURCE, classify_config, commit_batches

START = GeneratorConfig().start
CONFIG = GeneratorConfig(
    seed=9,
    orders=120,
    customers=25,
    end=START + timedelta(days=2),
    malformed_rate=0.02,
    late_rate=0.02,
    out_of_order_rate=0.05,
    duplicate_rate=0.03,
)

CLEAN_MARTS = """
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE TABLE analytics.fct_orders AS SELECT * FROM (VALUES
  ('o1', 'c1', 'USD', 100.00, 100.00, 0.00, 90.00),
  ('o2', 'c1', 'EUR', 50.00, 0.00, 0.00, 0.00)
) t(order_id, customer_id, currency, total_amount, captured_amount,
   refunded_amount, net_sales_amount);
CREATE TABLE analytics.fct_order_items AS SELECT * FROM (VALUES ('i1', 'o1', 'p1', 2))
  t(order_item_id, order_id, product_id, quantity);
CREATE TABLE analytics.dim_products AS SELECT * FROM (VALUES ('p1')) t(product_id);
CREATE TABLE analytics.dim_customers AS SELECT * FROM (VALUES ('c1')) t(customer_id);
CREATE TABLE analytics.fct_payments AS SELECT * FROM (VALUES ('o1')) t(order_id);
CREATE TABLE analytics.mart_daily_sales AS SELECT * FROM (VALUES (90.00)) t(net_sales);
"""


@pytest.fixture(scope="module")
def pristine(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("quality") / "demo"
    run_batch_demo(CONFIG, root, batches=2)
    return root


@pytest.fixture()
def root(pristine: Path, tmp_path: Path) -> Path:
    target = tmp_path / "demo"
    shutil.copytree(pristine, target)
    return target


def with_marts(root: Path, sql: str = CLEAN_MARTS) -> None:
    con = duckdb.connect(str(root / "warehouse.duckdb"))
    try:
        con.execute(sql)
    finally:
        con.close()


@pytest.fixture()
def clean_root(root: Path) -> Path:
    with_marts(root)
    return root


def build(root: Path, client: DuckDBClient, **overrides: Any) -> QualityContext:
    s3 = LocalObjectStore(root / "lake" / "s3-local", name="s3-local")
    gcs = LocalObjectStore(root / "lake" / "gcs-local", name="gcs-local")
    defaults: dict[str, Any] = {
        "client": client,
        "source": SOURCE,
        "now": utcnow(),
        "primary_store": s3,
        "lake_stores": {"s3-local": s3, "gcs-local": gcs},
        "control_store": LocalObjectStore(root / "lake" / "control", name="control"),
    }
    return QualityContext(**{**defaults, **overrides})


@pytest.fixture()
def client(clean_root: Path) -> Iterator[DuckDBClient]:
    db = DuckDBClient(clean_root / "warehouse.duckdb")
    yield db
    db.close()


def status(report_ctx: QualityContext, check_id: str) -> CheckStatus:
    return run_quality_suite(report_ctx).get(check_id).status


# --- healthy pipeline ---------------------------------------------------------------------


def test_check_ids_are_unique_and_cover_the_spec() -> None:
    ids = [check_id for check_id, _ in registered_checks()]
    assert len(ids) == len(set(ids))
    for needed in (
        "raw.event_id_unique",
        "raw.required_fields_not_null",
        "marts.amounts_in_bounds",
        "marts.currency_accepted",
        "marts.items_reference_orders_and_products",
        "marts.payments_reconcile_to_orders",
        "freshness.last_successful_load",
        "reconcile.loads_match_manifests",
        "reconcile.warehouse_row_count",
        "parity.cross_cloud",
    ):
        assert needed in ids


def test_healthy_pipeline_has_no_failures(clean_root: Path, client: DuckDBClient) -> None:
    report = run_quality_suite(build(clean_root, client))
    assert report.ok, report.format_text()
    assert report.count(CheckStatus.SKIP) == 0
    assert report.get("reconcile.warehouse_row_count").status is CheckStatus.PASS
    assert report.get("parity.cross_cloud").status is CheckStatus.PASS
    assert report.get("lake.batches_complete").status is CheckStatus.PASS


def test_optional_checks_are_skipped_not_failed_without_lake_inputs(
    clean_root: Path, client: DuckDBClient
) -> None:
    ctx = QualityContext(client=client, source=SOURCE)
    report = run_quality_suite(ctx)
    assert report.get("parity.cross_cloud").status is CheckStatus.SKIP
    assert report.get("lake.batches_complete").status is CheckStatus.SKIP
    assert report.get("raw.event_id_unique").status is CheckStatus.PASS
    assert report.ok


def test_missing_marts_are_reported_as_failures_not_crashes(root: Path) -> None:
    client = DuckDBClient(root / "warehouse.duckdb")  # dbt has not run: no analytics schema
    try:
        report = run_quality_suite(build(root, client))
    finally:
        client.close()
    broken = report.get("marts.amounts_in_bounds")
    assert broken.status is CheckStatus.FAIL
    assert broken.observed == "not evaluated"
    assert report.get("raw.event_id_unique").status is CheckStatus.PASS


# --- deliberately broken scenarios --------------------------------------------------------


def test_duplicate_rows_in_the_ledger_are_caught(clean_root: Path, client: DuckDBClient) -> None:
    client.execute(
        f"INSERT INTO {client.ref('raw_events')} SELECT * FROM {client.ref('raw_events')} LIMIT 3"
    )
    ctx = build(clean_root, client)
    assert status(ctx, "raw.event_id_unique") is CheckStatus.FAIL
    assert status(ctx, "reconcile.warehouse_row_count") is CheckStatus.FAIL


def test_stale_load_breaches_the_freshness_sla(clean_root: Path, client: DuckDBClient) -> None:
    ctx = build(clean_root, client, now=utcnow() + timedelta(days=3))
    report = run_quality_suite(ctx)
    assert report.get("freshness.last_successful_load").status is CheckStatus.FAIL
    assert report.get("freshness.latest_event").status is CheckStatus.WARN
    assert not report.ok


def test_frozen_clock_inside_sla_passes(clean_root: Path, client: DuckDBClient) -> None:
    ctx = build(clean_root, client, freshness_sla=timedelta(days=3650))
    assert status(ctx, "freshness.last_successful_load") is CheckStatus.PASS


def test_missing_manifest_in_one_cloud_fails_parity(clean_root: Path, client: DuckDBClient) -> None:
    gcs = LocalObjectStore(clean_root / "lake" / "gcs-local", name="gcs-local")
    batch_id = next(
        key.split("batch_id=")[1].split("/")[0] for key in gcs.list_objects("manifests/")
    )
    (clean_root / "lake" / "gcs-local" / manifest_key(SOURCE, batch_id)).unlink()
    report = run_quality_suite(build(clean_root, client))
    assert report.get("parity.cross_cloud").status is CheckStatus.FAIL
    assert batch_id in report.get("parity.cross_cloud").detail


def test_partial_batch_in_the_ledger_fails(clean_root: Path, client: DuckDBClient) -> None:
    control = LocalObjectStore(clean_root / "lake" / "control", name="control")
    key = next(iter(control.list_objects("ledger/")))
    data = json.loads(control.read_object(key))
    data["status"] = "PARTIAL"
    control.write_object(key, json.dumps(data).encode())
    result = run_quality_suite(build(clean_root, client)).get("lake.batches_complete")
    assert result.status is CheckStatus.FAIL
    assert data["batch_id"] in result.detail


def test_committed_but_unloaded_batch_is_a_warning(clean_root: Path, client: DuckDBClient) -> None:
    s3 = LocalObjectStore(clean_root / "lake" / "s3-local", name="s3-local")
    extra = classify_config(GeneratorConfig(seed=77, orders=20, customers=5)).valid
    commit_batches([extra], s3, run_id="late-arrival")
    result = run_quality_suite(build(clean_root, client)).get("reconcile.loads_match_manifests")
    assert result.status is CheckStatus.WARN
    assert "late-arrival-b00000" in result.detail


def test_failed_load_attempt_is_a_failure(clean_root: Path, client: DuckDBClient) -> None:
    client.execute(
        f"UPDATE {client.ref('load_audit')} SET status = 'FAILED', error = 'boom' "
        "WHERE batch_id = (SELECT MIN(batch_id) FROM " + client.ref("load_audit") + ")"
    )
    assert status(build(clean_root, client), "reconcile.loads_match_manifests") is CheckStatus.FAIL


def test_quarantine_rate_thresholds(clean_root: Path, client: DuckDBClient) -> None:
    assert status(build(clean_root, client), "ingest.quarantine_rate") is CheckStatus.PASS
    warn = build(clean_root, client, quarantine_warn_rate=0.001, quarantine_fail_rate=0.9)
    assert status(warn, "ingest.quarantine_rate") is CheckStatus.WARN
    fail = build(clean_root, client, quarantine_warn_rate=0.001, quarantine_fail_rate=0.002)
    assert status(fail, "ingest.quarantine_rate") is CheckStatus.FAIL


@pytest.mark.parametrize(
    ("sql", "check_id", "expected"),
    [
        (
            "UPDATE analytics.fct_orders SET total_amount = -5 WHERE order_id = 'o2'",
            "marts.amounts_in_bounds",
            CheckStatus.FAIL,
        ),
        (
            "UPDATE analytics.fct_orders SET currency = 'XXX' WHERE order_id = 'o2'",
            "marts.currency_accepted",
            CheckStatus.FAIL,
        ),
        (
            "DELETE FROM analytics.dim_products",
            "marts.items_reference_orders_and_products",
            CheckStatus.FAIL,
        ),
        (
            "UPDATE analytics.fct_orders SET captured_amount = 99 WHERE order_id = 'o1'",
            "marts.payments_reconcile_to_orders",
            CheckStatus.FAIL,
        ),
        (
            "UPDATE analytics.mart_daily_sales SET net_sales = 1",
            "marts.daily_sales_reconcile_to_orders",
            CheckStatus.FAIL,
        ),
        (
            "INSERT INTO analytics.fct_payments VALUES ('ghost')",
            "marts.payments_reference_orders",
            CheckStatus.WARN,
        ),
        (
            "DELETE FROM analytics.dim_customers",
            "marts.orders_reference_customers",
            CheckStatus.WARN,
        ),
    ],
)
def test_mart_level_violations(
    clean_root: Path, client: DuckDBClient, sql: str, check_id: str, expected: CheckStatus
) -> None:
    client.execute(sql)
    assert status(build(clean_root, client), check_id) is expected


# --- persistence and CLI ------------------------------------------------------------------


def test_results_are_persisted_to_the_warehouse_and_as_json(
    clean_root: Path, client: DuckDBClient
) -> None:
    ctx = build(clean_root, client)
    report = run_quality_suite(ctx, run_id="dq-test-1")
    persist_report(report, client, ctx.control_store, SOURCE)

    rows = client.query(
        f"SELECT check_id, status FROM {client.ref('quality_results')} WHERE run_id = 'dq-test-1'"
    )
    assert len(rows) == len(report.results)
    saved = json.loads(
        ctx.control_store.read_object(f"quality/source={SOURCE}/run_id=dq-test-1.json")  # type: ignore[union-attr]
    )
    assert saved["ok"] is True and saved["counts"]["FAIL"] == 0


def test_cli_exit_codes(root: Path, capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    assert quality_main(["--demo-dir", str(tmp_path / "nothing")]) == 2
    capsys.readouterr()

    # No marts yet (dbt not run): mart checks fail -> exit 1.
    assert quality_main(["--demo-dir", str(root), "--no-persist"]) == 1
    assert "FAILED" in capsys.readouterr().out

    with_marts(root)
    assert quality_main(["--demo-dir", str(root), "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is True
    db = DuckDBClient(root / "warehouse.duckdb")
    try:
        assert db.query(f"SELECT COUNT(*) AS n FROM {db.ref('quality_results')}")[0]["n"] == len(
            out["results"]
        )
    finally:
        db.close()
