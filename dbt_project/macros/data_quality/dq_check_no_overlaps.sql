--NO Overlap chcek 
{% macro dq_check_no_overlaps(model, business_key, effective_date, end_date) %}

with ordered as (
    select
        {{ business_key }} as bk,
        {{ effective_date }} as eff_date,
        {{ end_date }} as end_date,
        lead({{ effective_date }}) over (partition by {{ business_key }} order by {{ effective_date }}) as next_eff
    from {{ model }}
),

err_rec as (
    select *
    from ordered
    where next_eff is not null
      and next_eff < end_date       -- overlap condition
)

select *
from err_rec

{% endmacro %}