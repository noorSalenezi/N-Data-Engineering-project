-- One row per valid payment attempt (failed attempts are real rows). Rows that break the provider's
-- contract (non-positive amount, unknown status, method or currency) go to qrn_payments instead.
select
    payment_id,
    order_id,
    amount,
    currency,
    method,
    wallet_provider,
    status,
    card_last4,
    created_at::timestamptz at time zone 'UTC'   as created_at_utc,
    updated_at::timestamptz at time zone 'UTC'   as updated_at_utc
from {{ source('raw', 'payments') }}
where {{ payment_is_valid() }}
