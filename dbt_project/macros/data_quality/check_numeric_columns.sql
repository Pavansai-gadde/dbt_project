--Checks for non-numeric or invalid numeric data in columns expected to be numeric.
{% macro check_numeric_columns(model_name, column_name) %}
    select
        {{ column_name }} as invalid_value
    from {{ ref(model_name) }}
    where not regexp_like({{ column_name }}, '^[0-9]+(\.[0-9]+)?$')
{% endmacro %}