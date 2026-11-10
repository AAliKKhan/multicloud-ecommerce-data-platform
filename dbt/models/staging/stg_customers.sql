{#
  Grain: one row per customer. Key: customer_id.
  Current attributes come from the most recent customer_created/updated event.
#}
with events as (
    select
        event_id,
        event_time,
        {{ json_str('payload', 'customer_id') }} as customer_id,
        {{ json_ts('payload', 'created_at') }} as created_at,
        {{ json_str('payload', 'country_code') }} as country_code,
        {{ json_str('payload', 'city') }} as city,
        {{ json_str('payload', 'customer_segment') }} as customer_segment,
        {{ json_bool('payload', 'is_active') }} as is_active
    from {{ source('raw', 'raw_events') }}
    where event_type in ('customer_created', 'customer_updated')
),

ranked as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by event_time desc, event_id desc
        ) as state_rank
    from events
)

select
    customer_id,
    created_at,
    country_code,
    city,
    customer_segment,
    is_active,
    event_time as last_updated_at
from ranked
where state_rank = 1
