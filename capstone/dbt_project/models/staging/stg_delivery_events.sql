-- One row per event: duplicates (redelivered events) removed, the three device time formats
-- parsed into one UTC timestamp.
with source as (
    select * from {{ source('raw', 'delivery_events') }}
),

parsed as (
    select
        event_id,
        order_id,
        store_id,
        city,
        courier_id,
        event_type,
        case
            when regexp_full_match(event_time, '\d{13}') then make_timestamp(event_time::bigint * 1000)
            else event_time::timestamptz at time zone 'UTC'
        end                                                  as event_at_utc,
        ingest_time::timestamptz at time zone 'UTC'          as ingested_at_utc,
        lat,
        lon
    from source
)

select
    *,
    {{ to_riyadh('event_at_utc') }}                          as event_at_riyadh,
    datediff('second', event_at_utc, ingested_at_utc)        as ingest_delay_s
from parsed
qualify row_number() over (partition by event_id order by ingested_at_utc) = 1
