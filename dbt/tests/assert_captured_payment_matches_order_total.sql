-- Payment amounts must reconcile to their order: a captured payment equals the
-- order total (the generator never produces partial captures), and refunds
-- never exceed what was captured.
select order_id, total_amount, captured_amount, refunded_amount
from {{ ref('fct_orders') }}
where (captured_amount > 0 and captured_amount <> total_amount)
   or refunded_amount > captured_amount
