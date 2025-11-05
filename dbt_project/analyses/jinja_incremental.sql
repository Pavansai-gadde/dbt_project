{% set initload = 1 %}
{% set last_load =3 %}
{% set cols_list = ['"sales_id"','"payment_method"','"gross_amount"'] %}

select 
    {% for i in cols_list %}

        {{i}}  {% if not loop.last %} , {% endif %}

    {% endfor %}

from {{ ref('bronze_sales') }}

{% if initload == 1 %}
where "date_sk" > {{ last_load }}
{% endif %}