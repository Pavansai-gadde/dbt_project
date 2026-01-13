--compare counts between the tables  
{% macro compare_counts_between_layers(source_model, target_model) %}
    
    {% set sql_query %}
    select
        '{{ source_model }}' as source_model,
        '{{ target_model }}' as target_model,
        (select count(*) from {{ ref(source_model) }}) as source_count,
        (select count(*) from {{ ref(target_model) }}) as target_count,
        (select count(*) from {{ ref(source_model) }}) - (select count(*) from {{ ref(target_model) }}) as difference
    {% endset %}

    {% set result = run_query(sql_query).rows[0] %}
    {{ return({"src_count":result[2],"trg_count":result[3],"diff":result[4]})}}

{% endmacro %}  