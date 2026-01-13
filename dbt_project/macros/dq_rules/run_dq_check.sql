{% macro run_dq_checks_v2(table_name, layer) %}

    
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
            rule_id,
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
    create temp table {{ temp_table_name }} as
    with base as (
         SELECT
            {{ unique_key }},
            {# Add a column per rule dynamically #}
            {% for rule in dq_rules -%}
                IFF(
                    NOT ({{ rule['RULE_EXPRESSION'] }}),
                    'FAIL',
                    'SUCCESS'
                ) AS {{ rule['RULE_TYPE'] }}_{{ rule['RULE_ID'] }}
                {% if not loop.last %},{% endif %}
            {%- endfor -%}

        FROM {{ ref(table_name) }} 
    ),
    unpivoted as (
            SELECT 
            {{ unique_key }},
            rule_name,
            status,
        FROM base
        UNPIVOT (
            status FOR rule_name IN (
            {% for rule in dq_rules -%}
                {{ rule['RULE_TYPE'] }}_{{ rule['RULE_ID'] }}
                {% if not loop.last %},{% endif %}
            {%- endfor -%}                
            )
        )
    )
    select {{ unique_key }},
    r.table_name,
    r.column_name,
    r.rule_id,
    r.rule_type,
    u.status,
    case when u.status='FAIL' then r.error_message
    else null end as error_message,
    r.severity,
    r.threshold
    from unpivoted u 
    left join {{ source('config','dq_rules') }} r
    on u.rule_name=concat(r.rule_type,'_',r.rule_id)
    WHERE r.active_flag = TRUE
    AND UPPER(r.table_name) = UPPER('{{ table_name }}')
    AND UPPER(r.layer_name) = UPPER('{{ layer }}')

        
    {% endset %}
       
    
    {{ log(validate_sql, info=True)}}
{# 
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
      #}
         

{% endmacro %}