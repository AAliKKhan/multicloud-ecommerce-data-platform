# Data dictionary

All data is synthetic. Grain and key are given for every table. Business
metric definitions live in one place: `dbt/models/docs.md` (rendered in dbt
docs) and are implemented in `fct_orders`.

## Raw layer (loaded by Python)

| Table | Grain | Key | Notes |
|---|---|---|---|
| `raw.raw_events` | one row per valid event | `event_id` | Partitioned by `event_date` and clustered by `event_type, event_id` on BigQuery. `payload` is the original payload as JSON text. Idempotent `MERGE` guarantees uniqueness. |
| `raw.load_audit` | one row per load attempt | `(batch_id, attempt)` | Counts, status and error for every attempt; the source of load auditability. |

## Staging (views)

| Model | Grain | Key |
|---|---|---|
| `stg_orders` | one row per order, current state (highest `order_version`) | `order_id` |
| `stg_order_items` | one row per order line | `order_item_id` |
| `stg_payments` | one row per payment **event** (ledger) | `event_id` |
| `stg_customers` | one row per customer, latest attributes | `customer_id` |
| `stg_inventory_events` | one row per inventory movement | `inventory_event_id` |
| `stg_products` | one row per product (from the `products` seed) | `product_id` |

## Intermediate (views)

| Model | Grain | Key |
|---|---|---|
| `int_order_payment_status` | one row per order with payment activity | `order_id` |
| `int_order_items_enriched` | one row per order line with product + qualifying flag | `order_item_id` |
| `int_daily_inventory_movements` | one row per day, product, location | `(movement_date, product_id, location_id)` |

## Marts (tables)

| Model | Grain | Key |
|---|---|---|
| `fct_orders` | one row per order | `order_id` |
| `fct_order_items` | one row per order line | `order_item_id` |
| `fct_payments` | one row per payment event (ledger) | `event_id` |
| `fct_inventory_movements` | one row per movement | `inventory_event_id` |
| `dim_customers` | one row per customer (type 1) | `customer_id` |
| `dim_products` | one row per product (type 1) | `product_id` |
| `mart_daily_sales` | one row per order date (UTC) | `order_date` |
| `mart_product_performance` | one row per product (qualifying orders) | `product_id` |
| `mart_payment_health` | one row per date of terminal payment outcome | `payment_date` |
| `mart_inventory_risk` | one row per product and location | `(product_id, location_id)` |

## Metric definitions

- **Qualifying order:** current status is not `cancelled` and a payment was captured.
- **GMV:** sum of `subtotal` (before discounts) over non-cancelled orders.
- **Net sales:** for qualifying orders, `subtotal - discount_amount - refunds`. Tax and shipping excluded; cancelled orders and their refunds excluded.
- **Order count:** distinct qualifying orders. **AOV:** net sales / order count.
- **Payment success rate:** captured payments / (captured + failed) payments, each `payment_id` counted once at its first terminal outcome.
- **Inventory on hand:** sum of signed movements per product and location.
  `stock_status`: `out_of_stock` (<= 0), `low` (< `low_stock_threshold`, default 20), `ok`.

## Data-quality behaviour to know

- Events failing validation are quarantined (never silently dropped). Orphans
  caused by quarantined parents show up as dbt **warnings** on relationship
  tests (payments without orders, orders without customers) and as negative
  on-hand warnings. Hard failures are reserved for broken keys and broken
  metric reconciliation.
- Late events are kept and flagged (`is_late`); order state uses
  `order_version`, so late arrivals cannot overwrite newer state.
