{% macro run_dq_checks(table_name, layer) %}
{% if execute %}
    {{ log("Running DQ Validations on "~ table_name , info=True)}} 
    {% set temp_table_name = 'temp_' ~ table_name  %}
    
    {# {% set invocation_id = uuid4().hex %} #}
    
    {% set invocation_id = invocation_id %}

    {{ log(" Invocation ID " ~ invocation_id , info=True)}}


    {% set dq_rules_query %}
        SELECT 
            table_name,
            unique_key,
            column_name,
            rule_type,
            rule_expression,
            layer_name
        FROM {{ source('config','dq_rules') }} 
        WHERE active_flag = TRUE
        AND UPPER(table_name) = UPPER('{{ table_name }}')
        AND UPPER(layer_name) = UPPER('{{ layer }}')
    {% endset %}

    {% set dq_rules = run_query(dq_rules_query).rows %}

    {% if dq_rules | length == 0 %}
        {{ log("No DQ rules found for table " ~ table_name ~ " in layer " ~ layer, info=True) }}
        {% do return(None) %}
    {% endif %}

    {% set unique_key = dq_rules[0][1] %}
        
    {% set validate_sql %}
        create or replace temp table {{ temp_table_name }} as
        select *
        ,case 
            when error_message is null  then 'SUCCESS'
            else 'FAIL'
        end as status 
        from (
        select s.{{ unique_key }},
        r.rule_id,
        r.unique_key,
        r.table_name,
        r.column_name,
        r.layer_name,
        r.rule_type,
        r.rule_expression,
        case when not (
			CASE 
            {%- for rule in dq_rules -%}             
                {% set rule_type = rule['RULE_TYPE'] %}
                {% set rule_expression = rule['RULE_EXPRESSION'] %}
                WHEN r.rule_type = '{{ rule_type }}' THEN ({{ rule_expression }})
            {%- endfor %}
            ELSE TRUE 
            END 
        ) then r.error_message
        else null 
        end as error_message,
        r.severity,
        r.threshold
        FROM {{ ref(table_name) }} as s
        left join {{ source('config','dq_rules') }} as r    
        on r.table_name = '{{ table_name | upper}}'
        and r.active_flag=TRUE   )
        
    {% endset %}
       
    
    {# {{ log(validate_sql, info=True)}} #}

    {% set validated_records = run_query(validate_sql).rows %}

    {% set fail_sql %}

        select count(*) as fail_count from {{ temp_table_name }} where status='FAIL'

    {% endset %}

    

    {% set failed_records = run_query(fail_sql).rows %}

    
        
     {% if failed_records is not none %}
        {% set fail_count = failed_records[0][0] %}
    {% else %}
        {% set fail_count = 0 %}
    {% endif %}

    {% if fail_count > 0 %}

        {{ log(fail_count|string ~ " failed rows detected", info=True) }}

        {% do helper_handle_failed_records(invocation_id,unique_key,temp_table_name) %}

    {% endif %} 

    {{ log(" Running DQ Logs", info=True) }}
    {% do log_dq_execution(invocation_id,temp_table_name) %}
     
         
{% endif %}
{% endmacro %}