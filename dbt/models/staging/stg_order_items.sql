{#
  Grain: one row per order line. Key: order_item_id.
  Items are exploded from the items array of order_created events.
#}
select
    {{ json_str('item', 'order_item_id') }} as order_item_id,
    {{ json_str('e.payload', 'order_id') }} as order_id,
    {{ json_str('item', 'product_id') }} as product_id,
    {{ json_int('item', 'quantity') }} as quantity,
    {{ json_money('item', 'unit_price') }} as unit_price,
    {{ json_money('item', 'line_discount') }} as line_discount,
    {{ json_money('item', 'line_total') }} as line_total
from {{ source('raw', 'raw_events') }} as e
{{ cross_join_json_array('e.payload', 'items', 'item') }}
where e.event_type = 'order_created'
