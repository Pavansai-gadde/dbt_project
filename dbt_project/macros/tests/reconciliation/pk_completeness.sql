{% test pk_completeness(model, compare_model, key, variant_key=false) %}

{% if variant_key %}

  with landing_cte as (
    select distinct data:{{ key }}::string as src_key
    from {{ compare_model }}
    where data:{{ key }} is not null
  )

{% else %}

  with landing_cte as (
    select distinct {{ key }}::string as src_key
    from {{ compare_model }}
    where {{ key }} is not null
  )

{% endif %},

raw_cte as (
    select distinct {{ key }}::string as raw_key
    from {{ model }}
    where {{ key }} is not null
)

  select l.src_key
  from landing_cte l
  left join raw_cte r
    on l.src_key = r.raw_key
  where r.raw_key is null



{% endtest %}
