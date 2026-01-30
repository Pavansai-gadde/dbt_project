{% macro generate_sal(source_model,left_hk_col,right_hk_col,src_ldts,src_source) %}

{# Create source columns list #}
{% set source_columns %}
    {{ dbt_utils.generate_surrogate_key([left_hk_col, right_hk_col]) }} as sal_hk,
    {{ left_hk_col }} as left_hk,
    {{ right_hk_col }} as right_hk,
    {{ src_ldts }},        
    {{ src_source }}
{% endset %}

with source_data as (
select {{ source_columns }}
from {{ ref(source_model) }}
)
select *
from source_data 

{% if is_incremental() %}
where not exists(
    select 1 
    from {{ this }} as target 
    where target.left_hk = source_data.left_hk
    and target.right_hk = source_data.right_hk
) 
{% endif %}
 
{% endmacro %}