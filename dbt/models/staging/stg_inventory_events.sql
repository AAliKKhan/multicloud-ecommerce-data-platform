{#
  Grain: one row per inventory movement. Key: inventory_event_id.
  quantity_delta is signed: receipts positive, reservations negative.
#}
select
    {{ json_str('payload', 'inventory_event_id') }} as inventory_event_id,
    event_id,
    event_type as movement_type,
    {{ json_str('payload', 'product_id') }} as product_id,
    {{ json_str('payload', 'location_id') }} as location_id,
    {{ json_int('payload', 'quantity_delta') }} as quantity_delta,
    {{ json_str('payload', 'reason') }} as reason,
    event_time as movement_time
from {{ source('raw', 'raw_events') }}
where event_type in ('stock_received', 'stock_reserved', 'stock_adjusted')
