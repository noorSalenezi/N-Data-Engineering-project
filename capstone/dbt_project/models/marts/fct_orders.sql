-- Grain: one row per order. Delivery timings exist for the last week only (event stream retention).
with lines as (
    select order_id, count(*) as lines, sum(quantity) as units, sum(net_amount) as items_amount
    from {{ ref('fct_order_items') }}
    group by order_id
)

select
    o.order_id,
    o.order_date,
    o.created_at_riyadh,
    coalesce(c.customer_id, {{ var('unknown_key') }})                      as customer_id,
    o.store_id,
    o.status,
    o.payment_method,
    o.promo_code,
    coalesce(l.lines, 0)                                                   as lines,
    coalesce(l.units, 0)                                                   as units,
    coalesce(l.items_amount, 0)                                            as items_amount,
    o.delivery_fee,
    round(coalesce(l.items_amount, 0) + o.delivery_fee, 2)                 as order_total,
    p.paid_amount,
    p.payment_attempts,
    p.final_payment_status,
    d.delivered_at_utc,
    round(datediff('second', o.created_at_utc, d.delivered_at_utc) / 60.0, 1)  as delivery_minutes,
    case when d.delivered_at_utc is not null
         then datediff('second', o.created_at_utc, d.delivered_at_utc) <= {{ var('sla_minutes') }} * 60
    end                                                                    as within_sla
from {{ ref('stg_orders') }} o
left join lines l using (order_id)
left join {{ ref('int_payments_per_order') }} p using (order_id)
left join {{ ref('int_delivery_timeline') }} d using (order_id)
left join {{ ref('dim_customers') }} c on c.customer_id = o.customer_id
