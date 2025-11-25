--Validate date format 
--Validate if a date column conforms to a specific format (e.g. YYYY-MM-DD).

{% macro check_date_format(model, column_name, format='YYYY-MM-DD') %}
    select
        {{ column_name }} as invalid_date
    from {{ ref(model) }}
    where try_cast({{ column_name }} as date) is null
{% endmacro %}