select store_id, store_name_en, store_name_ar, city, lat, lon, opened_on
from {{ ref('stg_stores') }}
