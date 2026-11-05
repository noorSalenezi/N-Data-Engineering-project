-- One row per order. The app stores UTC as text; Qafila reports in Riyadh time.
select
    order_id,
    customer_id,
    store_id,
    status,
    payment_method,
    promo_code,
    delivery_fee,
    created_at::timestamp                                as created_at_utc,
    {{ to_riyadh('created_at::timestamp') }}             as created_at_riyadh,
    {{ to_riyadh('created_at::timestamp') }}::date       as order_date,
    updated_at::timestamp                                as updated_at_utc
from {{ source('raw', 'orders') }}
