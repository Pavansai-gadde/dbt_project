{% test aggregation_validation(model,compare_model,column_name, variant_key=false) %}

{% if variant_key %}

  with landing_cte as (
    select sum(data:{{ column_name }}::number) as src_total
    from {{ compare_model }}  )
{% else %}

  with landing_cte as (
    select sum({{ column_name }})::number as src_total
    from {{ compare_model }}  )

{% endif %},

raw_cte as (
    select sum({{ column_name }})::number as trg_total
    from {{ model }}
)

  select *
  from landing_cte l, raw_cte r
  where l.src_total!=r.trg_total

{% endtest %}