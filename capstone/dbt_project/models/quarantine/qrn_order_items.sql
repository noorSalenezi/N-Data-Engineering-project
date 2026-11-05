-- Order lines that fail validation are kept here, with the reason, instead of silently
-- disappearing or breaking the build. Someone owns this table and empties it.
select
    *,
    'non-positive quantity' as reject_reason
from {{ ref('stg_order_items') }}
where quantity <= 0
