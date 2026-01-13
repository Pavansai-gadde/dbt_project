{% macro aggregation_check(model,compare_model,column_name) %}


    {% set sql_query %}
    with landing_cte as (
        select sum({{ column_name }}) as src_total
        from {{ ref(compare_model) }}  )
    ,
    raw_cte as (
        select sum({{ column_name }}) as trg_total
        from {{ ref(model) }}
    )

    select src_total,trg_total ,src_total-trg_total as diff 
    from landing_cte l, raw_cte r
    {% endset %}

    {% set result = run_query(sql_query).rows[0] %}
    {{ return({"src_total":result[0],"trg_total":result[1],"diff":result[2]})}} 


{% endmacro %}