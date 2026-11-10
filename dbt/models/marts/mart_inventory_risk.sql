{#
  Grain: one row per (product_id, location_id). Key: that pair.
  on_hand = sum of signed movements (opening stock arrives as stock_received).
  stock_status: out_of_stock (<= 0), low (< var low_stock_threshold), ok.
#}
with on_hand as (
    select
        product_id,
        location_id,
        sum(quantity_delta) as units_on_hand,
        max(movement_time) as last_movement_time
    from {{ ref('fct_inventory_movements') }}
    group by product_id, location_id
)

select
    h.product_id,
    h.location_id,
    coalesce(p.product_name, 'unknown') as product_name,
    coalesce(p.category, 'unknown') as category,
    h.units_on_hand,
    h.last_movement_time,
    case
        when h.units_on_hand <= 0 then 'out_of_stock'
        when h.units_on_hand < {{ var('low_stock_threshold') }} then 'low'
        else 'ok'
    end as stock_status
from on_hand as h
left join {{ ref('dim_products') }} as p on p.product_id = h.product_id
