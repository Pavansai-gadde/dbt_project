{% macro get_rules_from_table(model_name) -%}

{% set result = '' %}

{% if execute %}
    {% set rule_query %}
        select rule_expression,RULE_NAME,COLUMN_NAME
        from {{ source('config','dq_rules') }}
        where model_name = '{{ model_name.upper() }}'
        and is_active=TRUE 
    {% endset %}

    {% set result = run_query(rule_query) %}
    {% do log("model_name  " ~ model_name.upper() , info=True) %}
    {% do log("result  " ~ result , info=True) %}
{% endif %}

{{ return({"rule_result":result })}} 

{%- endmacro %}