{#
  Grain: one row per payment EVENT (authorized / captured / failed / refunded).
  Key: event_id. Chosen as an event ledger so refunds and retries stay visible;
  payment-level outcomes are derived in mart_payment_health.
#}
select
    event_id,
    payment_id,
    order_id,
    payment_status,
    payment_method,
    amount,
    currency,
    provider_reference,
    failure_category,
    payment_time,
    cast(payment_time as date) as payment_date
from {{ ref('stg_payments') }}
