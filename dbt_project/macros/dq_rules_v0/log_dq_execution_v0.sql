{% macro log_dq_execution_v0(table_name, column_name, rule_type, rule_expression, status, failed_count, error_message) %}

    {% set insert_query %}
    INSERT INTO dbt_project_dev.config.DQ_LOG (
        table_name,
        column_name,
        rule_type,
        rule_expression,
        status,
        failed_count,
        error_message,
        run_timestamp
    )
    VALUES (
        '{{ table_name }}',
        '{{ column_name }}',
        '{{ rule_type }}',
        '{{ rule_expression | replace("'", "''") }}',
        '{{ status }}',
        {{ failed_count }},
        '{{ error_message | replace("'", "''") }}',
        CURRENT_TIMESTAMP()
    );

    {% endset %}

    {# {{ log(insert_query, info=True)}} #}
    {% do run_query(insert_query) %}

{% endmacro %}
