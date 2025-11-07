{% set source_model = ref('bronze_customer') %}
{% set compare_to = ref('bronze_product') %}

select 1 as validation_error,
a.src_cnt as source_count,
b.trg_cnt as compare_count
from 
(select count(*) as src_cnt from {{ source_model }}) a full join 
(select count(*) as trg_cnt from {{ compare_to }} ) b on true 
where a.src_cnt != b.trg_cnt