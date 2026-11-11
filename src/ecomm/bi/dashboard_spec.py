"""The business dashboard as data: questions (native SQL over the published marts) and layout.

Metric definitions are not re-derived here: every number comes from a dbt mart, whose definitions
live in docs/data-dictionary.md (net sales, qualifying orders, AOV, payment success rate, stock
status). The SQL only selects, sums over days, or ranks. The dashboard follows docs/07 section 7.3.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

COLLECTION_NAME = "E-commerce analytics (synthetic demo)"
DASHBOARD_NAME = "Business overview (synthetic demo data)"
DATABASE_NAME = "E-commerce marts (synthetic)"

NOTICE = (
    "**All data here is synthetic demo data** produced by this project's event generator. "
    "Numbers come from the curated dbt marts (never raw events); definitions are in "
    "`docs/data-dictionary.md`. **Net sales** = subtotal - discounts - refunds for qualifying "
    "orders (not cancelled, payment captured); tax and shipping excluded. **AOV** = net sales / "
    "orders. **Payment success rate** = captured / (captured + failed). "
    "Stock is low below 20 units."
)


@dataclass(frozen=True)
class Question:
    key: str
    name: str
    sql: str
    display: str  # Metabase display type: scalar, line, bar, pie, table
    settings: dict[str, Any] = field(default_factory=dict)
    # Grid placement on Metabase's 24-column grid.
    row: int = 0
    col: int = 0
    width: int = 12
    height: int = 6


QUESTIONS: tuple[Question, ...] = (
    Question(
        "kpi_net_sales",
        "Net sales (total)",
        "select coalesce(sum(net_sales), 0) as net_sales from analytics.mart_daily_sales",
        "scalar",
        {"scalar.field": "net_sales", "column_settings": {'["name","net_sales"]': {"prefix": "$"}}},
        row=2,
        col=0,
        width=6,
        height=3,
    ),
    Question(
        "kpi_orders",
        "Orders (total)",
        "select coalesce(sum(order_count), 0) as orders from analytics.mart_daily_sales",
        "scalar",
        {"scalar.field": "orders"},
        row=2,
        col=6,
        width=6,
        height=3,
    ),
    Question(
        "kpi_aov",
        "Average order value",
        "select round(sum(net_sales) / nullif(sum(order_count), 0), 2) as aov "
        "from analytics.mart_daily_sales",
        "scalar",
        {"scalar.field": "aov", "column_settings": {'["name","aov"]': {"prefix": "$"}}},
        row=2,
        col=12,
        width=6,
        height=3,
    ),
    Question(
        "kpi_payment_success",
        "Payment success rate",
        "select round(100.0 * sum(successful_payments) / nullif(sum(terminal_payments), 0), 1) "
        "as success_percent from analytics.mart_payment_health",
        "scalar",
        {
            "scalar.field": "success_percent",
            "column_settings": {'["name","success_percent"]': {"suffix": "%"}},
        },
        row=2,
        col=18,
        width=6,
        height=3,
    ),
    Question(
        "net_sales_by_day",
        "Net sales per day",
        "select order_date, net_sales from analytics.mart_daily_sales order by order_date",
        "line",
        {"graph.dimensions": ["order_date"], "graph.metrics": ["net_sales"]},
        row=5,
        col=0,
    ),
    Question(
        "orders_by_day",
        "Orders per day",
        "select order_date, order_count as orders, cancelled_orders as cancelled "
        "from analytics.mart_daily_sales order by order_date",
        "bar",
        {"graph.dimensions": ["order_date"], "graph.metrics": ["orders", "cancelled"]},
        row=5,
        col=12,
    ),
    Question(
        "aov_by_day",
        "Average order value per day",
        "select order_date, round(average_order_value::numeric, 2) as aov "
        "from analytics.mart_daily_sales order by order_date",
        "line",
        {"graph.dimensions": ["order_date"], "graph.metrics": ["aov"]},
        row=11,
        col=0,
    ),
    Question(
        "payment_outcomes_by_day",
        "Payment outcomes per day",
        "select payment_date, successful_payments as successful, failed_payments as failed, "
        "refunded_payments as refunded from analytics.mart_payment_health order by payment_date",
        "bar",
        {
            "graph.dimensions": ["payment_date"],
            "graph.metrics": ["successful", "failed", "refunded"],
            "stackable.stack_type": "stacked",
        },
        row=11,
        col=12,
    ),
    Question(
        "payment_success_rate",
        "Payment success rate per day (%)",
        "select payment_date, round((100 * success_rate)::numeric, 1) as success_percent "
        "from analytics.mart_payment_health order by payment_date",
        "line",
        {"graph.dimensions": ["payment_date"], "graph.metrics": ["success_percent"]},
        row=17,
        col=0,
    ),
    Question(
        "data_freshness",
        "Data freshness and last successful load",
        "select last_successful_load_at, batches_loaded, failed_load_attempts, published_at "
        "from analytics.data_freshness",
        "table",
        row=17,
        col=12,
    ),
    Question(
        "top_products",
        "Top 10 products by net sales",
        "select product_name, net_sales, units_sold from analytics.mart_product_performance "
        "order by net_sales desc, product_name limit 10",
        "bar",
        {"graph.dimensions": ["product_name"], "graph.metrics": ["net_sales"]},
        row=23,
        col=0,
        height=7,
    ),
    Question(
        "sales_by_category",
        "Net sales and units by category",
        "select category, sum(net_sales) as net_sales, sum(units_sold) as units "
        "from analytics.mart_product_performance group by category order by net_sales desc",
        "bar",
        {"graph.dimensions": ["category"], "graph.metrics": ["net_sales"]},
        row=23,
        col=12,
        height=7,
    ),
    Question(
        "low_stock",
        "Low or out-of-stock products",
        "select product_name, location_id, units_on_hand, stock_status "
        "from analytics.mart_inventory_risk where stock_status <> 'ok' "
        "order by units_on_hand, product_name, location_id limit 50",
        "table",
        row=30,
        col=0,
        width=24,
        height=7,
    ),
)

QUESTIONS_BY_KEY = {question.key: question for question in QUESTIONS}
