-- One row per order with a card / wallet payment: attempts and the final outcome.
select
    order_id,
    count(*)                                                           as payment_attempts,
    count(*) filter (where status = 'failed')                          as failed_attempts,
    max(amount) filter (where status in ('captured', 'refunded'))      as paid_amount,
    bool_or(status = 'refunded')                                       as is_refunded,
    arg_max(status, updated_at_utc)                                    as final_payment_status
from {{ ref('stg_payments') }}
group by order_id
