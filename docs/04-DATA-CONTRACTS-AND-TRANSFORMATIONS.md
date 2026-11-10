# 4. Data Contracts and Transformations

## 4.1 Event envelope

Every event must use a common envelope:

``` json
{
  "event_id": "uuid",
  "event_type": "order_created",
  "schema_version": 1,
  "event_time": "2026-11-10T10:15:30.000Z",
  "producer": "synthetic-commerce-generator",
  "correlation_id": "uuid",
  "payload": {}
}
```

Rules: - `event_id` is globally unique for a generated event and is the
primary deduplication key. - `event_time` is UTC ISO-8601. - Add
`ingested_at` and `processed_at` in the pipeline; do not overwrite
source event time. - `schema_version` is an integer and changes when a
breaking contract change is introduced. - Unknown fields should be
handled according to an explicit compatibility policy. - Do not put
payment card numbers, CVVs, passwords or real personal information in
payloads.

## 4.2 Core entities

### Customer

`customer_id`, `created_at`, `country_code`, `city`, `customer_segment`,
`is_active`

Use synthetic identifiers and fictional values. Avoid storing names,
email addresses or phone numbers unless a demo genuinely needs them.

### Product

`product_id`, `sku`, `product_name`, `category`, `unit_price`,
`currency`, `is_active`

### Order

`order_id`, `customer_id`, `order_status`, `order_time`, `currency`,
`subtotal`, `discount_amount`, `tax_amount`, `shipping_amount`,
`total_amount`

### Order item

`order_item_id`, `order_id`, `product_id`, `quantity`, `unit_price`,
`line_discount`, `line_total`

### Payment

`payment_id`, `order_id`, `payment_method`, `payment_status`, `amount`,
`currency`, `provider_reference`, `payment_time`

`provider_reference` is synthetic. Payment events represent simulated
outcomes only.

### Inventory

`inventory_event_id`, `product_id`, `location_id`, `quantity_delta`,
`reason`, `event_time`

## 4.3 Event types and payload expectations

-   `order_created`: order header and item snapshot.
-   `order_updated`: order ID and changed fields.
-   `order_cancelled`: order ID, cancellation time and reason code.
-   `payment_authorized`: payment ID, order ID, amount and currency.
-   `payment_captured`: payment ID, order ID, captured amount and time.
-   `payment_failed`: payment ID, order ID and failure category (no
    sensitive provider details).
-   `payment_refunded`: payment ID, order ID, refund amount and time.
-   `stock_received`, `stock_reserved`, `stock_adjusted`: product,
    location, quantity delta and reason.

Document each schema in code and validate at producer and consumer
boundaries.

## 4.4 Storage format and partitioning

-   Raw zone: newline-delimited JSON, append-only.
-   Validated zone: Parquet where supported.
-   BigQuery: typed columns, partition large event/fact tables by event
    date and cluster by common filter/join keys.
-   Partition raw object paths by event date/hour; do not partition by
    high-cardinality IDs.
-   Include `run_id`, `batch_id`, source topic, Kafka partition and
    offset where available.

## 4.5 Deduplication and idempotency

-   Deduplicate by `event_id`.
-   For order state, define deterministic precedence using event version
    or event timestamp plus a tie-breaker; do not blindly take arbitrary
    last row.
-   For payments, preserve the event ledger and derive current status
    separately.
-   BigQuery loads must use a staging table and `MERGE` on stable keys,
    or another documented idempotent strategy.
-   A replay of the same source batch must leave final business
    aggregates unchanged.
-   Keep duplicate counts as quality/observability metrics.

## 4.6 Late and out-of-order events

-   Preserve original `event_time`.
-   Use ingestion time for operational latency.
-   Define an event-time lateness threshold for streaming windows and
    document it in configuration.
-   Events arriving beyond the threshold must not silently disappear;
    route them to a late-event path or support batch backfill.
-   For mutable entities, use event versioning or a deterministic
    ordering rule.

## 4.7 Warehouse model

Recommended dbt layers:

**Staging** - `stg_orders` - `stg_order_items` - `stg_payments` -
`stg_customers` - `stg_products` - `stg_inventory_events`

Clean types, normalize statuses/currency, deduplicate and rename columns
consistently.

**Intermediate** - `int_order_items_enriched` -
`int_order_payment_status` - `int_daily_inventory_movements`

**Marts** - `fct_orders` --- one row per order. - `fct_order_items` ---
one row per order item. - `fct_payments` --- one row per payment event
or payment entity; choose and document grain. -
`fct_inventory_movements` --- one row per inventory movement. -
`dim_customers` --- one row per customer (or SCD2 if implemented). -
`dim_products` --- one row per product (or SCD2 if implemented). -
`mart_daily_sales` --- daily sales, order count and average order
value. - `mart_product_performance` --- units, gross sales, discounts
and net sales by product/category. - `mart_payment_health` --- payment
success/failure/refund rates. - `mart_inventory_risk` --- current stock
and low-stock indicators.

Every model must document its grain, key, business logic and refresh
behavior.

## 4.8 Business metric definitions

Define metrics once in dbt/docs: - **Gross merchandise value (GMV):**
sum of order item value before discounts, with explicit treatment of
cancellations and returns. - **Net sales:** completed/captured order
value minus discounts/refunds according to documented business rules. -
**Order count:** distinct qualifying order IDs; specify whether
cancelled orders are excluded. - **Average order value:** net sales
divided by qualifying distinct orders. - **Payment success rate:**
successful payment attempts divided by all terminal payment attempts;
specify whether retries count separately. - **Inventory on hand:**
opening quantity plus signed inventory movements.

Do not mix order totals, payment captures and refunds without clear
reconciliation rules.

## 4.9 Data quality checks

Minimum checks: - Required identifiers and timestamps are not null. -
`event_id` is unique after deduplication. - Amounts and quantities obey
documented bounds. - Currency values belong to an accepted set. - Order
items reference known orders and products. - Payment amounts reconcile
to their order within documented rules. - Event timestamps are parseable
and within plausible ranges. - Daily freshness meets the configured
SLA. - Source-to-target counts reconcile within expected handling
rules. - S3/GCS logical batch counts and checksums reconcile. -
Mart-level totals reconcile to their source facts.

Store failed row samples securely in quarantine; never expose secrets or
personal data in test output.
