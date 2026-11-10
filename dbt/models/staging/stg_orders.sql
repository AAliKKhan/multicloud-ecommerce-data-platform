{#
  Grain: one row per order. Key: order_id.
  Current order state = the event with the highest order_version
  (tie-break: latest event_time, then event_id), so late or out-of-order
  events can never overwrite a newer state.
#}
with events as (
    select
        event_id,
        event_type,
        event_time,
        payload,
        {{ json_str('payload', 'order_id') }} as order_id
    from {{ source('raw', 'raw_events') }}
    where event_type in ('order_created', 'order_updated', 'order_cancelled')
),

created as (
    select
        order_id,
        {{ json_str('payload', 'customer_id') }} as customer_id,
        {{ json_ts('payload', 'order_time') }} as order_time,
        {{ json_str('payload', 'currency') }} as currency,
        {{ json_money('payload', 'subtotal') }} as subtotal,
        {{ json_money('payload', 'discount_amount') }} as discount_amount,
        {{ json_money('payload', 'tax_amount') }} as tax_amount,
        {{ json_money('payload', 'shipping_amount') }} as shipping_amount,
        {{ json_money('payload', 'total_amount') }} as total_amount
    from events
    where event_type = 'order_created'
),

states as (
    select
        order_id,
        event_id,
        event_time,
        case event_type
            when 'order_created' then 'created'
            when 'order_cancelled' then 'cancelled'
            else {{ json_str('payload', 'order_status') }}
        end as order_status,
        {{ json_int('payload', 'order_version') }} as order_version,
        {{ json_str('payload', 'reason_code') }} as reason_code
    from events
),

ranked as (
    select
        *,
        row_number() over (
            partition by order_id
            order by order_version desc, event_time desc, event_id desc
        ) as state_rank
    from states
)

select
    c.order_id,
    c.customer_id,
    c.order_time,
    c.currency,
    c.subtotal,
    c.discount_amount,
    c.tax_amount,
    c.shipping_amount,
    c.total_amount,
    r.order_status,
    r.order_version,
    r.event_time as status_updated_at,
    case when r.order_status = 'cancelled' then r.reason_code end as cancellation_reason
from created as c
inner join ranked as r
    on r.order_id = c.order_id
   and r.state_rank = 1
