{{ config (materialized='table') }}

{% set rel = ref('validated_customers') %}
{% set cols = adapter.get_columns_in_relation(rel) %}

{# Convert to list of column names #}
{% set col_names = cols | map(attribute='name') | list %}

{# Exclude last 3 columns related to rules #}
{% set selected_cols = col_names[:-3] %}



with validated as (
    select 
    {% for col in selected_cols -%}
        {{ col }}{% if not loop.last %}, {% endif %}
    {%- endfor %}
     from  {{ ref('validated_customers') }} 
    where rule_id is null
)
select * from validated 
