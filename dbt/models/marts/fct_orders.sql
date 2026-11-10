{#
  Grain: one row per order. Key: order_id. Refresh: full rebuild (table).

  Metric rules (see docs block `metric_definitions`):
    is_qualifying    = not cancelled AND captured payment exists
    gmv_amount       = subtotal (before discounts) for non-cancelled orders, else 0
    net_sales_amount = subtotal - discount - refunds for qualifying orders, else 0
#}
select
    o.order_id,
    o.customer_id,
    o.order_time,
    cast(o.order_time as date) as order_date,
    o.currency,
    o.order_status,
    o.order_version,
    o.cancellation_reason,
    o.subtotal,
    o.discount_amount,
    o.tax_amount,
    o.shipping_amount,
    o.total_amount,
    coalesce(ps.payment_status, 'unpaid') as payment_status,
    coalesce(ps.captured_amount, 0) as captured_amount,
    coalesce(ps.refunded_amount, 0) as refunded_amount,
    coalesce(ps.failed_attempts, 0) as failed_payment_attempts,
    (o.order_status <> 'cancelled' and coalesce(ps.captured_amount, 0) > 0) as is_qualifying,
    case when o.order_status <> 'cancelled' then o.subtotal else 0 end as gmv_amount,
    case
        when o.order_status <> 'cancelled' and coalesce(ps.captured_amount, 0) > 0
            then o.subtotal - o.discount_amount - coalesce(ps.refunded_amount, 0)
        else 0
    end as net_sales_amount
from {{ ref('stg_orders') }} as o
left join {{ ref('int_order_payment_status') }} as ps on ps.order_id = o.order_id
