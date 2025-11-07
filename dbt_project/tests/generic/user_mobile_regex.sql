{% test user_mobile_regex(model, column_name) %}
SELECT * FROM {{ model }} WHERE NOT {{ column_name }} RLIKE '^\\+1-\\d{3}-\\d{3}-\\d{4}$'
{% endtest %}