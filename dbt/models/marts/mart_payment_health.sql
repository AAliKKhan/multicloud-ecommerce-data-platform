{#
  Grain: one row per payment_date, where payment_date is the date of each
  payment's TERMINAL outcome (captured or failed). Key: payment_date.
  Each payment_id counts once, so retries on a new payment_id count separately.
  Refunds are attributed to the date of the payment they refund.
  success_rate = captured / (captured + failed).
#}
with per_payment as (
    select
        payment_id,
        max(case when payment_status = 'captured' then 1 else 0 end) as was_captured,
        max(case when payment_status = 'failed' then 1 else 0 end) as was_failed,
        max(case when payment_status = 'refunded' then 1 else 0 end) as was_refunded,
        sum(case when payment_status = 'refunded' then amount else 0 end) as refunded_amount,
        sum(case when payment_status = 'captured' then amount else 0 end) as captured_amount,
        min(case when payment_status in ('captured', 'failed') then payment_time end) as terminal_time
    from {{ ref('fct_payments') }}
    group by payment_id
)

select
    cast(terminal_time as date) as payment_date,
    sum(was_captured) as successful_payments,
    sum(was_failed) as failed_payments,
    sum(was_captured) + sum(was_failed) as terminal_payments,
    round(
        cast(sum(was_captured) as {{ dbt.type_float() }})
        / nullif(sum(was_captured) + sum(was_failed), 0),
        4
    ) as success_rate,
    sum(was_refunded) as refunded_payments,
    sum(captured_amount) as captured_amount,
    sum(refunded_amount) as refunded_amount
from per_payment
where terminal_time is not null
group by cast(terminal_time as date)
