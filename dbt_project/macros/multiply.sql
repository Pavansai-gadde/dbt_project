{% macro multiply(table_name_arg, layer) %}


    {{ log("Running DQ Validations on "~ table_name_arg , info=True)}} 
    {% set temp_table_name = 'temp_' ~ table_name_arg  %}
    
    {# {% set invocation_id = uuid4().hex %} #}
    
    {% set invocation_id = invocation_id %}

    {{ log(" Invocation ID " ~ invocation_id , info=True)}}
{% if execute %}
    {% set dq_rules_query %}
        SELECT * 
        -- FROM {{ source('config','dq_rules') }} 
        FROM dbt_project_dev.config.dq_rules
        WHERE active_flag = TRUE

    {% endset %}
    {% set rendered_sql = render(dq_rules_query) %}
    {{ log(rendered_sql, info=True)}}

    {% set dq_rules = run_query(rendered_sql).rows %}
    
{% endif %}
    {#
    {% if dq_rules | length == 0 %}
        {{ log("No DQ rules found for table " ~ table_name ~ " in layer " ~ layer, info=True) }}
        {% do return(None) %}
    {% endif %}

    {% set unique_key = dq_rules[0][1] %}
    {{ log(" unique_key  " ~ unique_key , info=True)}} #}
{% endmacro %}