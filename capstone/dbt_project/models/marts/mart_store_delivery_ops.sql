-- Operations view. Grain: one row per store and Riyadh calendar day, for orders that appear in the
-- courier event stream (the stream keeps the last 7 days). Aggregated per store, never per courier.
with tracked as (
    select
        o.order_id,
        o.order_date,
        o.store_id,
        o.delivery_minutes,
        o.within_sla,
        d.assigned_at_utc,
        d.picked_up_at_utc,
        d.failed_at_utc,
        d.max_ingest_delay_s
    from {{ ref('fct_orders') }} o
    join {{ ref('int_delivery_timeline') }} d using (order_id)
)

select
    t.order_date,
    t.store_id,
    s.city,
    count(*)                                                                    as orders_tracked,
    count(t.delivery_minutes)                                                   as delivered_orders,
    count(t.failed_at_utc)                                                      as failed_deliveries,
    round(avg(t.delivery_minutes), 1)                                           as avg_delivery_minutes,
    round(quantile_cont(t.delivery_minutes, 0.9), 1)                            as p90_delivery_minutes,
    count(*) filter (where t.within_sla = false)                                as late_orders,
    round(avg(t.within_sla::int), 3)                                            as sla_rate,
    round(avg(datediff('second', t.assigned_at_utc, t.picked_up_at_utc) / 60.0), 1) as avg_pickup_wait_minutes,
    max(t.max_ingest_delay_s)                                                   as max_event_delay_s
from tracked t
join {{ ref('dim_stores') }} s on s.store_id = t.store_id
group by t.order_date, t.store_id, s.city
