-- SLA rates are shares: always between 0 and 1.
select 'mart_daily_city_sales' as model, order_date, city as key, sla_rate
from {{ ref('mart_daily_city_sales') }} where sla_rate < 0 or sla_rate > 1
union all
select 'mart_store_delivery_ops', order_date, store_id, sla_rate
from {{ ref('mart_store_delivery_ops') }} where sla_rate < 0 or sla_rate > 1
