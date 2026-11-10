{#
  Grain: one row per order line. Key: order_item_id.
  Adds product attributes and the order-level facts needed for sales metrics.
  An order is "qualifying" when it is not cancelled AND has a captured payment.
#}
select
    i.order_item_id,
    i.order_id,
    i.product_id,
    coalesce(p.product_name, 'unknown') as product_name,
    coalesce(p.category, 'unknown') as category,
    i.quantity,
    i.unit_price,
    i.quantity * i.unit_price as gross_amount,
    i.line_discount,
    i.line_total as net_amount,
    o.customer_id,
    o.order_time,
    cast(o.order_time as date) as order_date,
    o.order_status,
    (o.order_status <> 'cancelled' and coalesce(ps.captured_amount, 0) > 0) as is_qualifying
from {{ ref('stg_order_items') }} as i
inner join {{ ref('stg_orders') }} as o on o.order_id = i.order_id
left join {{ ref('stg_products') }} as p on p.product_id = i.product_id
left join {{ ref('int_order_payment_status') }} as ps on ps.order_id = i.order_id
