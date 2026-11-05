-- A courier phone can send an event late, never early. An event received before it happened means a
-- device clock or a timestamp parser is wrong.
select event_id, event_at_utc, ingested_at_utc
from {{ ref('stg_delivery_events') }}
where ingested_at_utc < event_at_utc
