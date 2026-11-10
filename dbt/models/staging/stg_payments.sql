{#
  Grain: one row per payment EVENT (a ledger). Key: event_id.
  Current payment state is derived separately (int_order_payment_status).
#}
select
    event_id,
    {{ json_str('payload', 'payment_id') }} as payment_id,
    {{ json_str('payload', 'order_id') }} as order_id,
    case event_type
        when 'payment_authorized' then 'authorized'
        when 'payment_captured' then 'captured'
        when 'payment_failed' then 'failed'
        when 'payment_refunded' then 'refunded'
    end as payment_status,
    {{ json_str('payload', 'payment_method') }} as payment_method,
    {{ json_money('payload', 'amount') }} as amount,
    {{ json_str('payload', 'currency') }} as currency,
    {{ json_str('payload', 'provider_reference') }} as provider_reference,
    {{ json_ts('payload', 'payment_time') }} as payment_time,
    {{ json_str('payload', 'failure_category') }} as failure_category
from {{ source('raw', 'raw_events') }}
where event_type in ('payment_authorized', 'payment_captured', 'payment_failed', 'payment_refunded')
