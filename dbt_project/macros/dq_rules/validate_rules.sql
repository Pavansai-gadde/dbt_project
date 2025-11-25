{% macro validate_rules(model_name) -%}

{% set model_name_up = model_name|upper() %}
{% set rules = [] %}

{% if execute %}
    -- Get rules for given model 
    {% set result = run_query("select rule_expression,rule_name
from dbt_project_dev.config.dq_rules
where model_name = '"~ model_name_up ~ "' and is_active=TRUE ")
     %}

    {% if result is none %}
        {{ exceptions.warn("run_query returned None — this model MUST be run with dbt run, not dbt compile.") }}
        {% set rules = [] %}
    {% else %}
        {% set rules = result.rows %}
    {% endif %}

{% endif %}


{% set rule_query %}
    -- Generate Validation query based on the rules
    select
        s.*,
        r.rule_id,
        r.rule_name,
        r.severity 
        from {{ ref(model_name) }} as s
        left join {{ source('config','dq_rules') }} r 
        on r.model_name = '{{ model_name_up }}'
        and is_active=TRUE 
        and not (case
        {%- for rule in rules -%}
            {% set rule_exp = rule[0] %}
            {% set rule_name = rule[1] %}           
            when  r.rule_name = '{{ rule_name }}'  then ({{ rule_exp }}) 
        {%- endfor -%}
        else TRUE 
        end )
     
{% endset %}


{{ return(rule_query) }}

{%- endmacro %}
