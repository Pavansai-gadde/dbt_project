--Start date
{% macro log_run_start() %}
    {{ return(convert_timezone('UTC', current_timestamp())) }}
{% endmacro %}