-- Weekly supplier price files. Week 39 renamed unit_cost -> cost_sar; week 38 used decimal commas
-- and Arabic-Indic digits. Costs that are not positive numbers are kept but flagged.
with source as (
    select * from {{ source('raw', 'supplier_prices') }}
)

select
    supplier_id,
    trim(sku)                                                    as sku,
    {{ normalize_arabic('product_name_ar') }}                    as product_name_ar,
    {{ parse_number('coalesce(unit_cost, cost_sar)') }}          as unit_cost_sar,
    coalesce(currency, 'SAR')                                    as currency,
    valid_from::date                                             as valid_from,
    min_order_qty::integer                                       as min_order_qty,
    regexp_extract(filename, 'W(\d+)', 1)::integer               as iso_week,
    coalesce(unit_cost, cost_sar)                                as cost_raw
from source
