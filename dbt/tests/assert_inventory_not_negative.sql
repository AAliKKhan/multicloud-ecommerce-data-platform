{{ config(severity='warn') }}
-- Warn (not fail): if a stock_received event is quarantined, on-hand can look
-- negative even though the pipeline behaved correctly. The warning surfaces it.
select product_id, location_id, units_on_hand
from {{ ref('mart_inventory_risk') }}
where units_on_hand < 0
