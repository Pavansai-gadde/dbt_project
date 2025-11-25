--One active  reccord check
{% macro dq_check_one_active_record(model, business_key, end_date) %}

with active as (
    select
        {{ business_key }} as bk,
        count(*) as active_count
    from {{ model }}
    where {{ end_date }} = '9999-12-31'
    group by {{ business_key }}
),

err_rec as (
    select *
    from active
    where active_count != 1
)

select *
from err_rec

{% endmacro %}