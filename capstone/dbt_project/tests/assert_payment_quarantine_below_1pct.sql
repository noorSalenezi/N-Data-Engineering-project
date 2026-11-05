-- Blocking: a few bad payment rows are quarantined and the pipeline goes on; more than 1% of the feed
-- means something is wrong at the provider, and revenue must not be published from it.
with q as (select count(*) as n from {{ ref('qrn_payments') }}),
     t as (select count(*) as n from {{ source('raw', 'payments') }})
select q.n as quarantined, t.n as received, round(100.0 * q.n / t.n, 2) as pct
from q, t
where q.n > 0.01 * t.n
