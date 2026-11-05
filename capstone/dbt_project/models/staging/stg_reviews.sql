select
    review_id,
    product_id,
    customer_id,
    rating,
    lang,
    review_text,
    created_at::timestamp as created_at
from {{ source('raw', 'reviews') }}
