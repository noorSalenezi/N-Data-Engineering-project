{{
    config(
        materialized='incremental',
        unique_key='order_line_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

-- Grain: one row per order line (order_id, line_no) with a positive quantity.
-- Incremental: reprocess orders updated within 3 days of the newest loaded update (late status changes).
with orders as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    where updated_at_utc > (select max(order_updated_at_utc) - interval 3 day from {{ this }})
    {% endif %}
),

customers as (
    select customer_id from {{ ref('dim_customers') }}
)

select
    i.order_line_id,
    i.order_id,
    i.line_no,
    o.order_date,
    coalesce(c.customer_id, {{ var('unknown_key') }})   as customer_id,
    o.store_id,
    i.product_id,
    o.status                                            as order_status,
    o.payment_method,
    o.promo_code,
    i.quantity,
    i.unit_price,
    i.discount,
    i.gross_amount,
    i.net_amount,
    o.updated_at_utc                                    as order_updated_at_utc
from orders o
join {{ ref('stg_order_items') }} i on i.order_id = o.order_id
left join customers c on c.customer_id = o.customer_id
where i.quantity > 0
