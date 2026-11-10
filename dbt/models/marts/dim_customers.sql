{#
  Grain: one row per customer. Key: customer_id. Type 1 (current values only).
  Lifetime figures use qualifying orders only.
#}
with lifetime as (
    select
        customer_id,
        count(distinct case when is_qualifying then order_id end) as lifetime_orders,
        sum(net_sales_amount) as lifetime_net_sales,
        min(order_time) as first_order_time
    from {{ ref('fct_orders') }}
    group by customer_id
)

select
    c.customer_id,
    c.created_at,
    c.country_code,
    c.city,
    c.customer_segment,
    c.is_active,
    coalesce(l.lifetime_orders, 0) as lifetime_orders,
    coalesce(l.lifetime_net_sales, 0) as lifetime_net_sales,
    l.first_order_time
from {{ ref('stg_customers') }} as c
left join lifetime as l on l.customer_id = c.customer_id
