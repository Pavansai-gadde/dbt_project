{% test count_check(src_model,compare_to) %}

select 1 as validation_error,
a.src_cnt as source_count,
b.trg_cnt as compare_count
from 
(select count(*) as src_cnt from {{ ref(src_model) }}) a full join 
(select count(*) as trg_cnt from {{ ref(compare_to) }} ) b on true 
where a.src_cnt != b.trg_cnt

{% endtest %}