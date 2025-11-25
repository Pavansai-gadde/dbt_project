--Checks that string columns fit within expected max length.
{% macro check_string_length(model_name, column_name, max_length) %}
    select
        {{ column_name }} as invalid_value,
        length({{ column_name }}) as actual_length
    from {{ ref(model_name) }}
    where length({{ column_name }}) > {{ max_length }}
{% endmacro %}