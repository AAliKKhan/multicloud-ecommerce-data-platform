{#
  Grain: one row per product. Key: product_id. Type 1 (current values only).
#}
select
    product_id,
    sku,
    product_name,
    category,
    unit_price,
    currency,
    is_active
from {{ ref('stg_products') }}
