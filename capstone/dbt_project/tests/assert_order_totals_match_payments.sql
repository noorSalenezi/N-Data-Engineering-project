-- Singular test: for paid orders, what the provider captured must equal what the order says
-- it costs (within one halala). Returns the offending orders. Warns rather than fails: a handful
-- of mismatches are a finance follow-up, not a reason to stop the dashboard.
{{ config(severity='warn') }}

select order_id, order_total, paid_amount, round(paid_amount - order_total, 2) as difference
from {{ ref('fct_orders') }}
where paid_amount is not null
  and status = 'delivered'
  and abs(paid_amount - order_total) > 0.01
