-- One row per customer account. National IDs and IBANs never reach the warehouse: ingestion replaces the
-- national ID with person_token (keyed HMAC) and keeps the raw values in the PII vault. The token still
-- links duplicate accounts of the same person.
with source as (
    select * from {{ source('raw', 'customers') }}
)

select
    customer_id,
    min(customer_id) over (partition by person_token)        as person_id,     -- oldest account of this person
    person_token,
    full_name,
    lower(trim(email))                                        as email,
    {{ normalize_ksa_mobile('mobile') }}                      as mobile_e164,
    city,
    district,
    created_at::timestamp                                     as created_at,
    updated_at::timestamp                                     as updated_at,
    is_deleted = 1                                            as is_deleted
from source
