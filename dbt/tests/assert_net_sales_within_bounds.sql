-- Net sales can never be negative and can never exceed GMV for an order.
select order_id, gmv_amount, net_sales_amount
from {{ ref('fct_orders') }}
where net_sales_amount < 0
   or net_sales_amount > gmv_amount
