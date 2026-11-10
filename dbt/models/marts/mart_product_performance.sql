{#
  Grain: one row per product (qualifying orders only). Key: product_id.
  Refunds are NOT allocated to products (documented simplification), so
  net_sales here is before refunds.
#}
select
    product_id,
    product_name,
    category,
    sum(quantity) as units_sold,
    sum(gross_amount) as gross_sales,
    sum(line_discount) as discounts,
    sum(net_amount) as net_sales,
    count(distinct order_id) as orders_containing_product
from {{ ref('fct_order_items') }}
where is_qualifying
group by product_id, product_name, category
