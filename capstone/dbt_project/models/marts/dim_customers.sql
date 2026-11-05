-- Current state of each customer account, without direct identifiers: name, email, mobile,
-- national ID and IBAN stay in staging, which few people can read. Includes an "unknown"
-- member for orders whose customer was erased.
select
    customer_id,
    person_id,
    customer_id <> person_id                 as is_duplicate_account,
    city,
    district,
    created_at::date                         as signup_date,
    is_deleted
from {{ ref('stg_customers') }}

union all

select {{ var('unknown_key') }}, {{ var('unknown_key') }}, false, 'Unknown', 'Unknown', null, false
