{% test unknown_value_check(model, column, allowed) %}

select {{ column }}
from {{ model }}
where {{ column }} not in (
    {% for v in allowed %}
      '{{ v }}' {% if not loop.last %}, {% endif %}
    {% endfor %}
)

{% endtest %}
