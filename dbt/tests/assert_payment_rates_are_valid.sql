-- Rates must be fractions, and terminal payments = successful + failed.
select payment_date, success_rate, successful_payments, failed_payments, terminal_payments
from {{ ref('mart_payment_health') }}
where success_rate < 0
   or success_rate > 1
   or terminal_payments <> successful_payments + failed_payments
