-- What analysts may see: a stable person token (links duplicate accounts) and coarse attributes only.
select
    customer_id,
    person_token,
    customer_id <> person_id                      as is_duplicate_account,
    city,
    district,
    lower(split_part(email, '@', 2))              as email_domain,
    created_at::date                              as signup_date,
    is_deleted
from {{ ref('stg_customers') }}
