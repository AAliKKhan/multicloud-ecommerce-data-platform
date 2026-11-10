"""Self-contained HTML business dashboard built from the dbt marts.

This is the **local stand-in for the BI tool** (Metabase / Looker Studio): one
static HTML file with inline SVG charts, no JavaScript and no external
requests, so it can be opened offline and attached to a portfolio. It reads
the curated marts only (never raw events) and uses the documented metric
definitions. Every figure is synthetic and the page says so.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from html import escape
from pathlib import Path
from typing import Any

from ecomm.report.charts import bar_chart, hbar_chart, line_chart
from ecomm.storage.interface import ObjectStore
from ecomm.warehouse.client import WarehouseClient, WarehouseError
from ecomm.warehouse.schema import LOAD_AUDIT


class DashboardError(Exception):
    """The marts needed for the dashboard are not available."""


@dataclass
class DashboardData:
    generated_at: datetime
    daily: list[dict[str, Any]] = field(default_factory=list)
    payments: list[dict[str, Any]] = field(default_factory=list)
    products: list[dict[str, Any]] = field(default_factory=list)
    categories: list[tuple[str, float]] = field(default_factory=list)
    inventory_risk: list[dict[str, Any]] = field(default_factory=list)
    stock_counts: dict[str, int] = field(default_factory=dict)
    last_load: datetime | None = None
    quality_counts: dict[str, int] | None = None
    quality_run: str | None = None


def collect(
    client: WarehouseClient,
    now: datetime,
    control_store: ObjectStore | None = None,
    source: str = "commerce",
) -> DashboardData:
    a = client.ref_analytics
    data = DashboardData(generated_at=now)
    try:
        data.daily = client.query(
            f"SELECT order_date, order_count, orders_placed, cancelled_orders, gross_merchandise_value, "
            f"net_sales, average_order_value FROM {a('mart_daily_sales')} ORDER BY order_date"
        )
        data.payments = client.query(
            f"SELECT payment_date, successful_payments, failed_payments, terminal_payments, success_rate, "
            f"refunded_amount FROM {a('mart_payment_health')} ORDER BY payment_date"
        )
        data.products = client.query(
            f"SELECT product_name, category, units_sold, net_sales FROM {a('mart_product_performance')} "
            f"ORDER BY net_sales DESC, product_name LIMIT 10"
        )
        data.categories = [
            (str(r["category"]), float(r["net_sales"]))
            for r in client.query(
                f"SELECT category, SUM(net_sales) AS net_sales FROM {a('mart_product_performance')} "
                f"GROUP BY category ORDER BY net_sales DESC, category"
            )
        ]
        data.inventory_risk = client.query(
            f"SELECT product_name, location_id, units_on_hand, stock_status FROM {a('mart_inventory_risk')} "
            f"WHERE stock_status <> 'ok' ORDER BY units_on_hand, product_name, location_id LIMIT 12"
        )
        data.stock_counts = {
            str(r["stock_status"]): int(r["n"])
            for r in client.query(
                f"SELECT stock_status, COUNT(*) AS n FROM {a('mart_inventory_risk')} GROUP BY stock_status"
            )
        }
    except WarehouseError as exc:
        raise DashboardError(
            "marts not found: run 'dbt build' before building the dashboard"
        ) from exc

    try:
        last = client.query(
            f"SELECT MAX(finished_at) AS t FROM {client.ref(LOAD_AUDIT.name)} WHERE status = 'SUCCEEDED'"
        )[0]["t"]
        data.last_load = last
    except WarehouseError:
        data.last_load = None

    if control_store is not None:
        keys = control_store.list_objects(f"quality/source={source}/")
        if keys:
            latest = json.loads(control_store.read_object(keys[-1]))
            data.quality_counts = {k: int(v) for k, v in latest["counts"].items()}
            data.quality_run = str(latest["run_id"])
    return data


def _money(v: float) -> str:
    return f"${v:,.2f}"


def _short_money(v: float) -> str:
    return f"${v:,.0f}"


def _pct(v: float) -> str:
    return f"{v * 100:.1f}%"


def _day(v: Any) -> str:
    return v.strftime("%m-%d") if isinstance(v, date | datetime) else str(v)


def _kpi(label: str, value: str, note: str = "") -> str:
    note_html = f'<div class="note">{escape(note)}</div>' if note else ""
    return (
        f'<div class="kpi"><div class="label">{escape(label)}</div>'
        f'<div class="value">{escape(value)}</div>{note_html}</div>'
    )


def _card(title: str, body: str, note: str = "") -> str:
    note_html = f'<p class="note">{escape(note)}</p>' if note else ""
    return f'<section class="card"><h2>{escape(title)}</h2>{body}{note_html}</section>'


def render(d: DashboardData) -> str:
    net = sum(float(r["net_sales"]) for r in d.daily)
    gmv = sum(float(r["gross_merchandise_value"]) for r in d.daily)
    orders = sum(int(r["order_count"]) for r in d.daily)
    cancelled = sum(int(r["cancelled_orders"]) for r in d.daily)
    ok = sum(int(r["successful_payments"]) for r in d.payments)
    terminal = sum(int(r["terminal_payments"]) for r in d.payments)
    aov = net / orders if orders else 0.0
    success = ok / terminal if terminal else 0.0

    days = [_day(r["order_date"]) for r in d.daily]
    pdays = [_day(r["payment_date"]) for r in d.payments]

    kpis = "".join(
        [
            _kpi("Net sales", _money(net), "qualifying orders, after discounts and refunds"),
            _kpi("Gross merchandise value", _money(gmv), "before discounts, non-cancelled orders"),
            _kpi("Qualifying orders", f"{orders:,}", f"{cancelled:,} cancelled"),
            _kpi("Average order value", _money(aov), "net sales / qualifying orders"),
            _kpi(
                "Payment success rate", _pct(success), f"{ok:,} of {terminal:,} terminal payments"
            ),
        ]
    )

    inventory_rows = (
        "".join(
            f"<tr><td>{escape(str(r['product_name']))}</td><td>{escape(str(r['location_id']))}</td>"
            f"<td class='num'>{int(r['units_on_hand']):,}</td>"
            f"<td><span class='tag {escape(str(r['stock_status']))}'>{escape(str(r['stock_status']))}</span></td></tr>"
            for r in d.inventory_risk
        )
        or "<tr><td colspan='4'>No products below the low-stock threshold.</td></tr>"
    )
    stock = ", ".join(f"{escape(k)}: {v}" for k, v in sorted(d.stock_counts.items()))

    if d.last_load is not None:
        fresh = f"Last successful warehouse load: {d.last_load:%Y-%m-%d %H:%M:%S} UTC"
    else:
        fresh = "No successful warehouse load recorded."
    if d.quality_counts is not None:
        quality = f"Latest quality run {escape(d.quality_run or '')}: " + ", ".join(
            f"{escape(k)} {v}" for k, v in d.quality_counts.items()
        )
    else:
        quality = "No quality run recorded yet (run ecomm-quality)."

    cards = [
        _card(
            "Net sales per day",
            bar_chart(
                days, [float(r["net_sales"]) for r in d.daily], _short_money, "Net sales per day"
            ),
        ),
        _card(
            "Qualifying orders per day",
            bar_chart(
                days,
                [float(r["order_count"]) for r in d.daily],
                lambda v: f"{v:,.0f}",
                "Orders per day",
                "#0ea5e9",
            ),
        ),
        _card(
            "Average order value",
            line_chart(
                days,
                [float(r["average_order_value"] or 0) for r in d.daily],
                _money,
                "Average order value",
            ),
        ),
        _card(
            "Payment success rate",
            line_chart(
                pdays,
                [float(r["success_rate"] or 0) for r in d.payments],
                _pct,
                "Payment success rate",
                "#f59e0b",
            ),
            "captured / (captured + failed), by date of the terminal outcome",
        ),
        _card(
            "Top products by net sales",
            hbar_chart(
                [str(r["product_name"]) for r in d.products],
                [float(r["net_sales"]) for r in d.products],
                _short_money,
                "Top products",
            ),
            "before refunds (refunds are not allocated to products)",
        ),
        _card(
            "Net sales by category",
            hbar_chart(
                [c for c, _ in d.categories],
                [v for _, v in d.categories],
                _short_money,
                "Net sales by category",
                "#14b8a6",
            ),
        ),
        _card(
            "Inventory risk",
            f"<p class='note'>Locations by status - {stock or 'no data'}</p>"
            "<table><thead><tr><th>Product</th><th>Location</th><th>On hand</th><th>Status</th></tr></thead>"
            f"<tbody>{inventory_rows}</tbody></table>",
        ),
        _card("Data freshness and quality", f"<p>{escape(fresh)}</p><p>{quality}</p>"),
    ]

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>E-commerce data platform - business dashboard (synthetic data)</title>
<style>
body{{font-family:system-ui,Segoe UI,Arial,sans-serif;margin:0;background:#f1f5f9;color:#0f172a}}
header{{background:#0f172a;color:#fff;padding:18px 28px}}
header h1{{margin:0;font-size:20px}} header p{{margin:4px 0 0;color:#cbd5e1;font-size:13px}}
.banner{{background:#fef3c7;color:#92400e;padding:8px 28px;font-size:13px}}
main{{padding:20px 28px;max-width:1280px;margin:auto}}
.kpis{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;margin-bottom:16px}}
.kpi,.card{{background:#fff;border-radius:10px;padding:14px 16px;box-shadow:0 1px 2px #0001}}
.kpi .label{{font-size:12px;color:#64748b}} .kpi .value{{font-size:26px;font-weight:650;margin-top:4px}}
.note{{font-size:12px;color:#64748b;margin:4px 0 0}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(460px,1fr));gap:14px}}
.card h2{{font-size:15px;margin:0 0 8px}} svg{{width:100%;height:auto}}
table{{border-collapse:collapse;width:100%;font-size:13px}} th,td{{text-align:left;padding:5px 6px;border-bottom:1px solid #e2e8f0}}
td.num{{text-align:right}} .tag{{padding:2px 8px;border-radius:999px;font-size:11px}}
.tag.low{{background:#fef3c7;color:#92400e}} .tag.out_of_stock{{background:#fee2e2;color:#991b1b}}
footer{{padding:16px 28px;color:#64748b;font-size:12px}}
</style>
</head>
<body>
<header><h1>E-commerce data platform - business dashboard</h1>
<p>Built from the dbt marts. Generated {d.generated_at:%Y-%m-%d %H:%M:%S} UTC.</p></header>
<div class="banner">All data on this page is <strong>synthetic</strong>. This is a local static dashboard (DuckDB marts), a stand-in for Metabase; it is not a BigQuery or Metabase deployment.</div>
<main>
<div class="kpis">{kpis}</div>
<div class="grid">{"".join(cards)}</div>
</main>
<footer>Metric definitions: docs/data-dictionary.md. Net sales = subtotal - discounts - refunds on qualifying orders (not cancelled, payment captured); tax and shipping excluded.</footer>
</body>
</html>
"""


def build_dashboard(
    client: WarehouseClient, out: Path, now: datetime, control_store: ObjectStore | None = None
) -> Path:
    html = render(collect(client, now, control_store))
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8", newline="\n")
    return out
