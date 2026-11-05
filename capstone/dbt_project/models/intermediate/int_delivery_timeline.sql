-- One row per order that has delivery events: when each milestone happened (UTC).
select
    order_id,
    any_value(store_id)                                               as store_id,
    any_value(courier_id)                                             as courier_id,
    min(event_at_utc) filter (where event_type = 'assigned')          as assigned_at_utc,
    min(event_at_utc) filter (where event_type = 'picked_up')         as picked_up_at_utc,
    min(event_at_utc) filter (where event_type = 'delivered')         as delivered_at_utc,
    min(event_at_utc) filter (where event_type = 'failed')            as failed_at_utc,
    min(event_at_utc) filter (where event_type = 'cancelled')         as cancelled_at_utc,
    max(ingest_delay_s)                                               as max_ingest_delay_s
from {{ ref('stg_delivery_events') }}
group by order_id
