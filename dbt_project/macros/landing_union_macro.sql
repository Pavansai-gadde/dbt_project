{% macro landing_union_macro(landing_models,unique_key, order_by_col) %}
{% set column_names = generate_column_names(landing_models[0]) %}

with union_data as (

{%- for model_name in landing_models -%}
select  {{ column_names }} {{ order_by_col }}  from {{ ref(model_name) }}
{% if not loop.last %} union all {% endif %}
{% endfor %} 

)
select {{ column_names }} from (
    select *,
    row_number() over(partition by {{ unique_key }} order by {{ order_by_col }} desc) as rn
    from union_data
)
 where rn = 1
order by {{ unique_key }} 
{% endmacro %}  