{% set source_model = ref('raw_synd') %}
{% set landing_models=  ['landing_synd1','landing_synd2','landing_synd3'] %}
{% set unique_key='ID' %}
{% set order_col='FIVETRAN_TIMESTAMP' %}


select 1 as validation_error,
a.src_cnt as source_count, 
b.trg_cnt as compare_count
from 
(select count(*) as src_cnt from {{ source_model }}) a 
full join 
(select count(*) as trg_cnt from 
({{ landing_union_macro(landing_models,unique_key,order_col) }} )  ) b on true 
where a.src_cnt != b.trg_cnt


