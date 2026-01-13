{% macro helper_handle_failed_records_v0(table_name, fail_sql,rule_expression, apply_action) %}

    {% set failed_table = 'error_capture.error' %}
    {% set original_table = source('landing', table_name) %}

    {% if apply_action == 'Y' %}

        {% set queries %}
        
        CREATE TABLE IF NOT EXISTS {{ failed_table }} LIKE {{ original_table }};

        INSERT INTO {{ failed_table }}
        {{ fail_sql }}

        {% endset %}

       
        {% do run_query(queries) %}

        {% set update_query %}
            UPDATE {{ original_table }}
            SET DQ_STATUS = 'INVALID'
            WHERE NOT({{ rule_expression }})
        {% endset %}

         {% do run_query(update_query) %}

    {% else %}
        {{ log("apply_invalid_action = N → No inserts or updates executed", info=True) }}
    {% endif %}

{% endmacro %}