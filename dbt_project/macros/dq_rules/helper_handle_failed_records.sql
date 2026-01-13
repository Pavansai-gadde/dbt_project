{% macro helper_handle_failed_records(invocation_id,unique_key,temp_table_name) %}
    

    {% set insert_query %}

    insert into error_capture.dq_error(
    RUN_ID
    ,RECORD_ID
    ,TABLE_NAME
    ,COLUMN_NAME
    ,RULE_ID
    ,ERROR_MESSAGE
    ,SEVERITY
    ,THRESHOLD
    )
    select '{{ invocation_id }}',
    {{ unique_key }},
    table_name,
    column_name,
    rule_id,
    error_message,
    severity,
    threshold
    from {{ temp_table_name }} 
    where status='FAIL';

    {% endset %}

    {# {{ log(insert_query, info=True)}} #}
    {% do run_query(insert_query) %}

    {{ log("Inserted failed rows into error table ", info=True) }}

{% endmacro %}
