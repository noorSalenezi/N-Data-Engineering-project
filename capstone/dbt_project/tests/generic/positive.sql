{# A custom generic test: fails for every row where the column is zero or negative. #}
{% test positive(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} <= 0
{% endtest %}
