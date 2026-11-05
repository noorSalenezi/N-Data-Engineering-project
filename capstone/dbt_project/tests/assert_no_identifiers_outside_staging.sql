-- Governance gate: no direct identifier may appear as a column in marts or the analyst schema.
-- depends_on: {{ ref('analyst_customers') }}
-- depends_on: {{ ref('dim_customers') }}
-- depends_on: {{ ref('mart_store_delivery_ops') }}
select table_schema, table_name, column_name
from information_schema.columns
where table_schema in ('marts', 'analyst')
  and lower(column_name) in ('national_id', 'iban', 'full_name', 'email', 'mobile', 'mobile_e164', 'card_last4', 'courier_id')
