{% set model_name = ref('bronze_customer') %}
{% set column_data = adapter.get_columns_in_relation(model_name) %} 
select 
{% for col in column_data -%}
    {% if not col.name.startswith('Fivetran') %}
    {{ col.name }}{%- if not loop.last -%}, {% endif %}
    {% endif %}
{% endfor %}
from {{ model_name }}