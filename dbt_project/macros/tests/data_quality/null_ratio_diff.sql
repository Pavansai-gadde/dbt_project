{% test null_ratio_diff(model, compare_model, column, threshold=0.05) %}

with src as (
  select (sum(case when {{ column }} is null then 1 else 0 end) / count(*)) as ratio
  from {{ model }}
),
tgt as (
  select (sum(case when {{ column }} is null then 1 else 0 end) / count(*)) as ratio
  from {{ compare_model }}
)

select * from src, tgt
where abs(src.ratio - tgt.ratio) > {{ threshold }}

{% endtest %}
