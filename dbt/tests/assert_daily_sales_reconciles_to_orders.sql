-- Mart totals must equal the totals of the facts they are built from.
-- Returns a row (= failure) when any total differs.
with mart as (
    select
        sum(net_sales) as mart_net_sales,
        sum(gross_merchandise_value) as mart_gmv,
        sum(order_count) as mart_order_count,
        sum(orders_placed) as mart_orders_placed
    from {{ ref('mart_daily_sales') }}
),

fact as (
    select
        sum(net_sales_amount) as fact_net_sales,
        sum(gmv_amount) as fact_gmv,
        count(distinct case when is_qualifying then order_id end) as fact_order_count,
        count(*) as fact_orders_placed
    from {{ ref('fct_orders') }}
)

select
    mart.mart_net_sales,
    fact.fact_net_sales,
    mart.mart_gmv,
    fact.fact_gmv
from mart
cross join fact
where mart.mart_net_sales <> fact.fact_net_sales
   or mart.mart_gmv <> fact.fact_gmv
   or mart.mart_order_count <> fact.fact_order_count
   or mart.mart_orders_placed <> fact.fact_orders_placed
