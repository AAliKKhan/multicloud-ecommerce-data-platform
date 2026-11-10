{#
  Grain: one row per (movement_date, product_id, location_id).
  Key: that triple. Quantities are in units; reserved is reported positive.
#}
select
    cast(movement_time as date) as movement_date,
    product_id,
    location_id,
    sum(case when movement_type = 'stock_received' then quantity_delta else 0 end) as units_received,
    sum(case when movement_type = 'stock_reserved' then -quantity_delta else 0 end) as units_reserved,
    sum(case when movement_type = 'stock_adjusted' then quantity_delta else 0 end) as units_adjusted,
    sum(quantity_delta) as net_movement,
    count(*) as movement_count
from {{ ref('stg_inventory_events') }}
group by cast(movement_time as date), product_id, location_id
