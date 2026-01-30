{% macro generate_nhs(source_model,src_pk,src_payload,src_eff,src_ldts,src_source) %}
 
{# Create source columns list #}
{% set source_columns %}
    {{ src_pk }} ,
    {% for col in src_payload %}
    {{ col }},
    {% endfor %}
    {{ src_eff }},
    {{ src_ldts }},        
    {{ src_source }}
{% endset %}

with source_data as (
select {{ source_columns }}
from {{ ref(source_model) }}
{% if is_incremental() %}
  where {{ src_ldts }} > (select coalesce(max({{ src_ldts }}), '1900-01-01') from {{ this }})
{% endif %}
)
select 
    {{ source_columns }}
from source_data

{% endmacro %} 