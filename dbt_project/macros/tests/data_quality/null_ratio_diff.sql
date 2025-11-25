{% test null_ratio_diff(model, compare_model, column, threshold=0.05) %}

with src as (
  select (sum(case when {{ column }} is null then 1 else 0 end) / count(*)) as src_ratio
  from {{ model }}
),
trg as (
  select (sum(case when {{ column }} is null then 1 else 0 end) / count(*)) as trg_ratio
  from {{ compare_model }}
)

select * from src, trg
where abs(src.src_ratio - trg.trg_ratio) > {{ threshold }}

{% endtest %}
