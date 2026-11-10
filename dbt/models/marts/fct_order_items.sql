{#
  Grain: one row per order line. Key: order_item_id. Refresh: full rebuild.
  gross_amount = quantity * unit_price; net_amount = gross - line discount.
#}
select
    order_item_id,
    order_id,
    product_id,
    product_name,
    category,
    customer_id,
    order_time,
    order_date,
    order_status,
    is_qualifying,
    quantity,
    unit_price,
    gross_amount,
    line_discount,
    net_amount
from {{ ref('int_order_items_enriched') }}
