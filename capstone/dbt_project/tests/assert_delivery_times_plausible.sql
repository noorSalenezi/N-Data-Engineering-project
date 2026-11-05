-- Delivery must take a positive time and less than 3 hours; anything else is a bad timestamp, not a slow courier.
select order_id, delivery_minutes
from {{ ref('fct_orders') }}
where delivery_minutes is not null
  and (delivery_minutes <= 0 or delivery_minutes > 180)
