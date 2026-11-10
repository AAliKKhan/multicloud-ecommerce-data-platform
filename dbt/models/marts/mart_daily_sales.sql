{#
  Grain: one row per order_date (UTC date of order_time). Key: order_date.
  Refresh: full rebuild.
  order_count and average_order_value use QUALIFYING orders only.
#}
select
    order_date,
    count(*) as orders_placed,
    count(distinct case when is_qualifying then order_id end) as order_count,
    sum(case when order_status = 'cancelled' then 1 else 0 end) as cancelled_orders,
    sum(gmv_amount) as gross_merchandise_value,
    sum(case when is_qualifying then discount_amount else 0 end) as discount_amount,
    sum(case when is_qualifying then refunded_amount else 0 end) as refunded_amount,
    sum(net_sales_amount) as net_sales,
    round(
        sum(net_sales_amount)
        / nullif(count(distinct case when is_qualifying then order_id end), 0),
        2
    ) as average_order_value
from {{ ref('fct_orders') }}
group by order_date
