{% test field_value_match(model, compare_model, by_key, columns) %}

with j as (
  select s.{{ by_key }} as key,
    {% for c in columns %}
      s.{{ c }} as s_{{ c }},
      t.{{ c }} as t_{{ c }} {% if not loop.last %},{% endif %}
    {% endfor %}
  from {{ model }} s
  join {{ compare_model }} t using({{ by_key }})
)

select *
from j
where
{% for c in columns %}
  coalesce(s_{{ c }}, '__NULL__') != coalesce(t_{{ c }}, '__NULL__')
  {% if not loop.last %} or {% endif %}
{% endfor %}

{% endtest %}
