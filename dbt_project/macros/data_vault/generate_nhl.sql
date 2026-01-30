{% macro generate_nhl(source_model,src_pk,src_fk,src_ldts,src_source) %}

{# Create source columns list #}
{% set source_columns %}
    {{ src_pk }} ,
    {% for col in src_fk %}
    {{ col }},
    {% endfor %}
    {{ src_ldts }},        
    {{ src_source }}
{% endset %}

with source_data as (
select {{ source_columns }}
from {{ ref(source_model) }}
{% if is_incremental() %}
  where {{ src_ldts }} > (select coalesce(max({{ src_ldts }}), '1900-01-01') from {{ this }})
{% endif %}
),
deduplicated_data as (
    select 
        {{ source_columns }},
        row_number() over(
            partition by {{ src_pk }}
            order by {{ src_ldts }} desc
        ) as rn 
    from source_data
    qualify rn = 1
)
select 
    {{ source_columns }}
from deduplicated_data

{% endmacro %}