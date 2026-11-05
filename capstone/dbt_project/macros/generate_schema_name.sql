{# Use the folder's schema name as is (staging, marts, ...) instead of dbt's default "<target>_<custom>". #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name else target.schema }}
{%- endmacro %}
