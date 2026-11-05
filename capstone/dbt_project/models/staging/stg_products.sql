select
    product_id,
    trim(sku)                              as sku,
    name_en                                as product_name_en,
    {{ normalize_arabic('name_ar') }}      as product_name_ar,
    category,
    brand,
    unit_price                             as list_price,
    description_en,
    description_ar,
    is_active = 1                          as is_active,
    updated_at::timestamp                  as updated_at
from {{ source('raw', 'products') }}
