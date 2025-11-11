{% macro generate_column_names(src_model) %}

{% set model_name = ref(src_model) %}
{% set column_data = adapter.get_columns_in_relation(model_name) %} 

{%- for col in column_data -%}
    {%- if not col.name.startswith('FIVETRAN') -%}
    {{ col.name }}{%- if not loop.last -%}, {% endif %}
    {% endif %}
{%- endfor -%}


{% endmacro %} 