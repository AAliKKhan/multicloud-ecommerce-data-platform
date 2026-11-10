from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from ecomm.report.charts import bar_chart, empty_chart, hbar_chart, line_chart
from ecomm.report.cli import main as report_main
from ecomm.report.dashboard import DashboardError, build_dashboard, collect, render
from ecomm.warehouse.duckdb_client import DuckDBClient
from ecomm.warehouse.schema import LOAD_AUDIT

NOW = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)

MARTS = """
CREATE SCHEMA analytics;
CREATE TABLE analytics.mart_daily_sales AS SELECT * FROM (VALUES
  (DATE '2026-11-01', 10, 12, 2, 1500.00, 1200.50, 120.05),
  (DATE '2026-11-02',  5,  6, 1,  800.00,  700.00, 140.00)
) t(order_date, order_count, orders_placed, cancelled_orders, gross_merchandise_value, net_sales, average_order_value);
CREATE TABLE analytics.mart_payment_health AS SELECT * FROM (VALUES
  (DATE '2026-11-01', 9, 1, 10, 0.9, 0.00),
  (DATE '2026-11-02', 4, 1,  5, 0.8, 10.00)
) t(payment_date, successful_payments, failed_payments, terminal_payments, success_rate, refunded_amount);
CREATE TABLE analytics.mart_product_performance AS SELECT * FROM (VALUES
  ('Deluxe Lamp', 'home', 30, 900.00),
  ('<script>alert(1)</script>', 'apparel', 5, 300.00)
) t(product_name, category, units_sold, net_sales);
CREATE TABLE analytics.mart_inventory_risk AS SELECT * FROM (VALUES
  ('Deluxe Lamp', 'loc_01', 3, 'low'),
  ('Vintage Cap', 'loc_02', 0, 'out_of_stock'),
  ('Eco Mug', 'loc_01', 400, 'ok')
) t(product_name, location_id, units_on_hand, stock_status);
"""


@pytest.fixture()
def client(tmp_path: Path) -> DuckDBClient:
    con = duckdb.connect(str(tmp_path / "w.duckdb"))
    con.execute(MARTS)
    con.close()
    db = DuckDBClient(tmp_path / "w.duckdb")
    db.ensure_table(LOAD_AUDIT)
    db.load_rows(
        LOAD_AUDIT,
        [
            {
                "load_id": "l1",
                "batch_id": "b1",
                "attempt": 1,
                "source": "commerce",
                "status": "SUCCEEDED",
                "expected_rows": 1,
                "rows_read": 1,
                "rows_inserted": 1,
                "rows_duplicate": 0,
                "started_at": NOW,
                "finished_at": NOW,
                "error": None,
            }
        ],
    )
    return db


def test_kpis_use_the_documented_metric_definitions(client: DuckDBClient) -> None:
    html = render(collect(client, NOW))
    assert "$1,900.50" in html  # net sales 1200.50 + 700.00
    assert "$2,300.00" in html  # GMV 1500 + 800
    assert ">15<" in html  # qualifying orders 10 + 5
    assert "$126.70" in html  # AOV = 1900.50 / 15
    assert "86.7%" in html  # 13 successful of 15 terminal payments
    assert "3 cancelled" in html


def test_page_is_labelled_synthetic_and_not_a_bi_deployment(client: DuckDBClient) -> None:
    html = render(collect(client, NOW))
    assert "synthetic" in html.lower()
    assert "stand-in for Metabase" in html


def test_page_is_self_contained(client: DuckDBClient) -> None:
    html = render(collect(client, NOW))
    assert "<script" not in html.replace("&lt;script&gt;", "")  # no JavaScript
    assert not re.search(r'(src|href)="https?://', html)  # no external requests
    assert html.count("<svg") >= 5


def test_user_controlled_text_is_escaped(client: DuckDBClient) -> None:
    html = render(collect(client, NOW))
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html


def test_inventory_table_lists_only_risky_stock(client: DuckDBClient) -> None:
    html = render(collect(client, NOW))
    assert "Vintage Cap" in html and "out_of_stock" in html
    assert "Eco Mug" not in html  # status ok is not at risk


def test_freshness_line_reports_the_last_load(client: DuckDBClient) -> None:
    assert "Last successful warehouse load: 2026-11-10 12:00:00 UTC" in render(collect(client, NOW))


def test_build_and_cli_write_the_file(client: DuckDBClient, tmp_path: Path) -> None:
    out = build_dashboard(client, tmp_path / "out" / "dash.html", NOW)
    assert out.read_text(encoding="utf-8").startswith("<!DOCTYPE html>")


def test_missing_marts_is_a_clear_error(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    root = tmp_path / "demo"
    root.mkdir()
    DuckDBClient(root / "warehouse.duckdb").close()
    with pytest.raises(DashboardError, match="dbt build"):
        c = DuckDBClient(root / "warehouse.duckdb", read_only=True)
        try:
            collect(c, NOW)
        finally:
            c.close()
    assert report_main(["--demo-dir", str(root)]) == 1
    assert "dbt build" in capsys.readouterr().out
    assert report_main(["--demo-dir", str(tmp_path / "nothing")]) == 2


# --- chart helpers -------------------------------------------------------------------------------------------


def test_charts_escape_labels_and_handle_empty_input() -> None:
    nasty = ['"><img src=x>']
    for svg in (
        bar_chart(nasty, [1.0], str, "t"),
        line_chart(nasty, [1.0], str, "t"),
        hbar_chart(nasty, [1.0], str, "t"),
    ):
        assert "<img" not in svg
    assert "No data" in empty_chart("t")
    assert "No data" in bar_chart([], [], str, "t")


def test_single_point_line_chart_does_not_divide_by_zero() -> None:
    assert "<polyline" in line_chart(["a"], [5.0], str, "t")


def test_all_zero_series_render() -> None:
    assert "<rect" in bar_chart(["a", "b"], [0.0, 0.0], str, "t")
