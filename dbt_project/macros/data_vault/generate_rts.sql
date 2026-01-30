{% macro generate_rts(source_model,src_pk,src_hashdiff,src_ldts,src_source) %}

{# Create source columns list #}
{% set source_columns %}
    {{ src_pk }} ,
    {{ src_hashdiff }},
    {{ src_ldts }},        
    {{ src_source }},
    TRUE as is_active,
    NULL as end_dts
{% endset %}

{# Insert new records from source if it doesn't already exist or if there is any change in hashdiff#}
with source_data as (
select {{ source_columns }}
from {{ ref(source_model) }}
{% if is_incremental() %}
  where {{ src_ldts }} > (select coalesce(max({{ src_ldts }}), '1900-01-01') from {{ this }})
{% endif %}
),
latest_data as (

    {% if is_incremental() %}
    select {{ source_columns }}
    from {{ this }}
    where is_active = TRUE 

    {% else %}
    select null as {{ src_pk }},
    null as {{ src_hashdiff }} 
    {% endif %}
),
changed_data as (
    select sd.*
    from source_data sd
    left join latest_data ld
    on sd.{{ src_pk }} = ld.{{ src_pk }}
    where ld.{{ src_pk }} is null
    or sd.{{ src_hashdiff }} != ld.{{ src_hashdiff }}
)



select 
    {{ source_columns }}
from changed_data

{% endmacro %}