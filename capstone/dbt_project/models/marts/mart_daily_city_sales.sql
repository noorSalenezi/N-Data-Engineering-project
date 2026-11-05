-- One row per Riyadh calendar day and city: the numbers on Qafila's daily dashboard.
select
    o.order_date,
    s.city,
    count(*)                                                                as orders,
    count(*) filter (where o.status = 'delivered')                          as delivered_orders,
    round(sum(o.items_amount) filter (where o.status = 'delivered'), 2)     as revenue_sar,
    round(avg(o.items_amount) filter (where o.status = 'delivered'), 2)     as avg_basket_sar,
    round(avg(o.within_sla::int), 3)                                        as sla_rate
from {{ ref('fct_orders') }} o
join {{ ref('dim_stores') }} s using (store_id)
group by all
