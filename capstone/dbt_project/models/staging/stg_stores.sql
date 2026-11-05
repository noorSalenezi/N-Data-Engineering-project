select
    store_id,
    city,
    name_en  as store_name_en,
    name_ar  as store_name_ar,
    lat,
    lon,
    opened_at::date as opened_on
from {{ source('raw', 'stores') }}
