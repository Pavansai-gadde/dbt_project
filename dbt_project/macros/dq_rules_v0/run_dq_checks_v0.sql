{% macro run_dq_checks_v0(table_name, layer) %}

    {% set dq_rules_query %}
        SELECT 
            table_name,
            column_name,
            rule_type,
            rule_expression,
            error_message,
            severity,
            active_flag,
            apply_invalid_action,
            layer_name
        FROM {{ source('config','dq_rules_v2') }}
        WHERE active_flag = TRUE
        AND UPPER(table_name) = UPPER('{{ table_name }}')
        AND UPPER(layer_name) = UPPER('{{ layer }}')
    {% endset %}

    {% set dq_rules = run_query(dq_rules_query).rows %}

    {% if dq_rules | length == 0 %}
        {{ log("No DQ rules found for table " ~ table_name ~ " in layer " ~ layer, info=True) }}
        {% do return(None) %}
    {% endif %}

    {% for r in dq_rules %}
        {% set col = r['COLUMN_NAME'] %}
        {% set rule_expression = r['RULE_EXPRESSION'] %}
        {% set rule_type = r['RULE_TYPE'] %}
        {% set error_message = r['ERROR_MESSAGE'] %}
        {% set apply_action = r['APPLY_INVALID_ACTION'] %}
        {% set layer_name = r['LAYER_NAME'] %}

        {{ log("Executing DQ Rule (" ~ layer_name ~ " layer): " ~ rule_type ~ " on " ~ table_name ~ "." ~ col, info=True) }}
        
        {% set fail_sql %}
            SELECT * 
            FROM {{ source(layer_name | lower, table_name | lower) }}
            WHERE NOT ({{ rule_expression }});

        {% endset %}
        {{ log(fail_sql, info=True)}}

        {% set failed_records = run_query(fail_sql) %}
        
        {% if failed_records is not none %}
            {% set fail_count = failed_records.rows | length %}
        {% else %}
            {% set fail_count = 0 %}
        {% endif %}

        {% if fail_count > 0 %}

            {{ log(fail_count|string ~ " failed rows detected", info=True) }}


            {% do helper_handle_failed_records_v0(table_name, fail_sql,rule_expression, apply_action) %}

            {% do log_dq_execution_v0(table_name, col, rule_type, rule_expression, 'FAILED', fail_count, error_message) %}	

        {% else %}
            {{ log("Rule PASSED: " ~ rule_type, info=True) }}

            {% do log_dq_execution_v0(table_name, col, rule_type, rule_expression, 'PASSED', 0, '') %}
        {% endif %}
        
    {% endfor %}
{% endmacro %}