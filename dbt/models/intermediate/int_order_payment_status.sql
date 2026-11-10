{#
  Grain: one row per order that has at least one payment event. Key: order_id.
  Derives the CURRENT payment state from the payment event ledger.
#}
with totals as (
    select
        order_id,
        sum(case when payment_status = 'authorized' then amount else 0 end) as authorized_amount,
        sum(case when payment_status = 'captured' then amount else 0 end) as captured_amount,
        sum(case when payment_status = 'refunded' then amount else 0 end) as refunded_amount,
        sum(case when payment_status = 'failed' then 1 else 0 end) as failed_attempts,
        min(payment_time) as first_payment_time,
        max(payment_time) as last_payment_time
    from {{ ref('stg_payments') }}
    group by order_id
)

select
    order_id,
    authorized_amount,
    captured_amount,
    refunded_amount,
    failed_attempts,
    first_payment_time,
    last_payment_time,
    case
        when captured_amount > 0 and refunded_amount >= captured_amount then 'refunded'
        when captured_amount > 0 and refunded_amount > 0 then 'partially_refunded'
        when captured_amount > 0 then 'captured'
        when failed_attempts > 0 then 'failed'
        when authorized_amount > 0 then 'authorized'
        else 'unpaid'
    end as payment_status
from totals
