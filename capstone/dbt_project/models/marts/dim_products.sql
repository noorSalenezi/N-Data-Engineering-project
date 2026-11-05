select
    product_id,
    sku,
    product_name_en,
    product_name_ar,
    category,
    brand,
    list_price,
    is_active
from {{ ref('stg_products') }}
