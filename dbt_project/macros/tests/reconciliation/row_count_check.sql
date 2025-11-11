{% test row_count_check(model,compare_model) %}

select 1 as validation_error,
a.src_cnt as source_count,
b.trg_cnt as compare_count
from 
(select count(*) as src_cnt from {{ model }} ) a full join 
(select count(*) as trg_cnt from {{ compare_model }} ) b on true 
where a.src_cnt != b.trg_cnt

{% endtest %}