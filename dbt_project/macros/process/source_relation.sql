{% macro source_relation(source_name,table_name) %}
    {% set relation=source(source_name,table_name) %}

    select * from {{ relation }}
{% endmacro %}