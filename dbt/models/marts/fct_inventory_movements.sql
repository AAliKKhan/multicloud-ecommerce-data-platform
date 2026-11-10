{#
  Grain: one row per inventory movement. Key: inventory_event_id.
#}
select
    inventory_event_id,
    movement_type,
    product_id,
    location_id,
    quantity_delta,
    reason,
    movement_time,
    cast(movement_time as date) as movement_date
from {{ ref('stg_inventory_events') }}
