{% test pk_completeness(model, compare_model, key) %}

select s.{{ key }}
from {{ model }} s
left join {{ compare_model }} t
  on s.{{ key }} = t.{{ key }}
where t.{{ key }} is null

{% endtest %}
