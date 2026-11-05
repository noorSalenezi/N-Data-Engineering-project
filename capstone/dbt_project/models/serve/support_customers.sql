-- What the support team may see: the customer's name to greet them, contact details masked.
-- National ID and IBAN are not in the warehouse at all (PII vault only).
select
    customer_id,
    full_name,
    regexp_replace(email, '^(.)[^@]*', '\1***')                      as email_masked,
    case when mobile_e164 is not null then '+9665•••••' || right(mobile_e164, 2) end as mobile_masked,
    city,
    district
from {{ ref('stg_customers') }}
where not is_deleted
