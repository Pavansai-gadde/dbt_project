{% macro get_sample_rows() %}

{% set sql_query %}
    select * from {{ ref("raw_orders") }} 
{% endset %}

{% set r = run_query(sql_query) %}

{% if r is not none %}
    {% set sample_rows = r.adapter_response.get("sample_rows",[]) %}
    {{ return(sample_rows) }}
{% else %}
    {{ return([]) }}
{% endif %}


{% endmacro %}