{% macro apply_dq_rules(model_name) -%}

{% set model_name_up = model_name|upper() %}


    {# {% set result = run_query("select rule_expression,rule_name
from dbt_project_dev.config.dq_rules
where model_name = '"~ model_name_up ~ "' and is_active=TRUE ")
     %} #}

    
    {% set result = get_rules_from_table(model_name_up) %}

    
    {% if result is none %}
        {{ exceptions.warn("run_query returned None — this model MUST be run with dbt run, not dbt compile.") }}
        {% set rules = [] %}
    {% else %}
        {% set rules = result['rule_result'].rows %}
    {% endif %}

{% set rule_query %} 
    select
        s.*,
        {%- for rule in rules -%}
        {% set rule_exp = rule[0] %}
        {% set rule_name = rule[1] %}
            case when not ( {{ rule_exp }} ) then '{{ rule_name }}' end 
                as validation_error_{{ loop.index }}
            {% if not loop.last %}, {% endif %}
        {%- endfor -%}
     from {{ ref(model_name) }} as s
{% endset %}


{{ return(rule_query) }}

{%- endmacro %}
