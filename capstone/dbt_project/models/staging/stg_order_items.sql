-- One row per order line. Negative quantities exist in the source (see the issue log);
-- they are kept here and handled by tests and the fact table.
select
    order_id,
    line_no,
    order_id * 100 + line_no                          as order_line_id,
    product_id,
    quantity,
    unit_price,
    discount,
    round(quantity * unit_price, 2)                   as gross_amount,
    round(quantity * unit_price - discount, 2)        as net_amount
from {{ source('raw', 'order_items') }}
