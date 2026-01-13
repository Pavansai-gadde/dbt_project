{% macro log_dq_execution(invocation_id,temp_table_name) %}
    

    {% set insert_query %}

    insert into DBT_PROJECT_DEV.CONFIG.DQ_LOG(
    run_id,
    TABLE_NAME
    ,COLUMN_NAME
    ,RULE_ID
    ,RULE_TYPE
    ,THRESHOLD
    ,TOTAL_COUNT
    ,SUCCESS_COUNT
    ,FAILURE_COUNT
    ,Success_percentage
    )
    select   
    '{{ invocation_id }}',
    table_name,
    column_name,
    -- listagg(customer_id ,', ')  WITHIN GROUP (ORDER BY customer_id) record_id_list ,
    rule_id,
    rule_type,
    threshold,
    -- status,
    count(*) as total_count,
    sum(case when status='SUCCESS' then 1 else 0 end ) as Success_count,
    sum(case when status='FAIL' then 1 else 0 end ) as Fail_count,
    ((Success_count/total_count)*100)::number as Success_percentage  
    from {{ temp_table_name }} 
    group by all

    {% endset %}

    {# {{ log(insert_query, info=True)}} #}
    {% do run_query(insert_query) %}

    {{ log("Inserted logs into DQ_LOG table ", info=True) }}

{% endmacro %}
