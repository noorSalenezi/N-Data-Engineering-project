{# Arabic text normalization for search and matching: drop tatweel and diacritics, unify alef forms. #}
{% macro normalize_arabic(col) -%}
    translate(regexp_replace({{ col }}, '[ـً-ْ]', '', 'g'), 'أإآ', 'ااا')
{%- endmacro %}

{# Parse numbers typed by humans: decimal commas and Arabic-Indic digits ("9,49", "٩٫٤٩") -> DOUBLE. #}
{% macro parse_number(col) -%}
    try_cast(replace(translate(trim({{ col }}), '٠١٢٣٤٥٦٧٨٩٫', '0123456789.'), ',', '.') AS DOUBLE)
{%- endmacro %}

{# Saudi mobile numbers in any of the app's formats -> E.164 (+9665XXXXXXXX), NULL if unrecognisable. #}
{% macro normalize_ksa_mobile(col) -%}
    CASE WHEN regexp_full_match(regexp_replace({{ col }}, '[^0-9]', '', 'g'), '(966|0)?5\d{8}')
         THEN '+966' || right(regexp_replace({{ col }}, '[^0-9]', '', 'g'), 9) END
{%- endmacro %}

{# Riyadh has no daylight saving time: local time is always UTC+3. #}
{% macro to_riyadh(ts) -%}
    ({{ ts }} + INTERVAL 3 HOUR)
{%- endmacro %}
