{% docs metric_definitions %}
Business metrics are defined once, here, and implemented in `fct_orders`.

- **Qualifying order:** order whose current status is not `cancelled` AND that has a captured payment.
- **Gross merchandise value (GMV):** sum of order `subtotal` (item value before discounts) over non-cancelled orders.
- **Net sales:** for qualifying orders, `subtotal - discount_amount - refunded_amount`. Tax and shipping are excluded. Cancelled orders (and their refunds) are excluded entirely.
- **Order count:** distinct qualifying orders.
- **Average order value (AOV):** net sales divided by the qualifying order count.
- **Payment success rate:** distinct payments captured divided by distinct payments that reached a terminal outcome (captured or failed). Authorizations and refunds are not terminal outcomes; a retry under a new `payment_id` counts as a separate attempt.
- **Inventory on hand:** sum of signed inventory movements per product and location.

All figures come from synthetic data. Late and out-of-order events never overwrite newer order state (ordered by `order_version`).
{% enddocs %}
