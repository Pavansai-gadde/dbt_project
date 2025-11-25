{% macro recon_key_match(model,compare_model,column_name) %}


    {% set sql_query %}
    with landing_cte as (
        select {{ column_name }} as src_key
        from {{ ref(compare_model) }} 
        where {{ column_name }} is not null )
    ,
    raw_cte as (
        select {{ column_name }} as trg_key
        from {{ ref(model) }}
        where {{ column_name }} is not null
    )

        select to_json(array_agg(l.src_key)) ,
        case when array_agg(l.src_key)=[] then 'Pass'
        else 'Fail' end as status
        from landing_cte l
        full join raw_cte r
            on l.src_key = r.trg_key
        where r.trg_key is null 
    {% endset %}

    {% set result = run_query(sql_query).rows[0] %}
    {{ return( {"missing_keys":result[0],"status":result[1] })}} 


{% endmacro %}