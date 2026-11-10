-- Item gross amounts must add up to the order subtotal, and item discounts to the order discount.
with items as (
    select
        order_id,
        sum(gross_amount) as items_gross,
        sum(line_discount) as items_discount
    from {{ ref('fct_order_items') }}
    group by order_id
)

select o.order_id, o.subtotal, i.items_gross, o.discount_amount, i.items_discount
from {{ ref('fct_orders') }} as o
inner join items as i on i.order_id = o.order_id
where o.subtotal <> i.items_gross
   or o.discount_amount <> i.items_discount
