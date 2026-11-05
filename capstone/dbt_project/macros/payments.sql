{# The payments contract (capstone/contracts/payments.odcs.yaml) as one predicate, shared by staging and quarantine. #}
{% macro payment_is_valid() -%}
    (amount > 0
     and currency = 'SAR'
     and status in ('captured', 'failed', 'refunded')
     and method in ('card', 'apple_pay', 'wallet'))
{%- endmacro %}
