{% test duplicate_check(model, keys) %}

select {{ keys | join(', ') }}, count(*) as cnt
from {{ model }}
group by {{ keys | join(', ') }}
having count(*) > 1

{% endtest %}
