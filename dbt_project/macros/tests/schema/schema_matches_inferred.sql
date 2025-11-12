{% test schema_matches_inferred(model, json_location, file_format) %}
 
-- Step 1: Get inferred schema from JSON files
with inferred as (
    select
        upper(column_name) as column_name,
        upper(type) as data_type
    from table(
        infer_schema(
            location => '{{ json_location }}',
            file_format => '{{ file_format }}'
        )
    ) order by order_id 
),
 
-- Step 2: Get actual schema from the landing table
actual as (
    select column_name,
    case when data_type='NUMBER' then 
    concat(data_type,'(',numeric_precision,', ',numeric_scale,')')
    else data_type
    end as data_type
    from {{ model.database }}.information_schema.columns
    where table_name = upper('{{ model.identifier }}')
    order by ordinal_position    
),
 
-- Step 3: Compare inferred vs actual
differences as (
    select
        'missing_in_table' as issue_type,
        i.column_name,
        i.data_type,
        null as actual_data_type
    from inferred i
    left join actual a using (column_name)
    where a.column_name is null
 
    union all
 
    select
        'extra_in_table' as issue_type,
        a.column_name,
        null as inferred_data_type,
        a.data_type
    from actual a
    left join inferred i using (column_name)
    where i.column_name is null
 
    union all
 
    select
        'type_mismatch' as issue_type,
        i.column_name,
        i.data_type as inferred_data_type,
        a.data_type as actual_data_type
    from inferred i
    join actual a using (column_name)
    where i.data_type != a.data_type
)
 
select *
from differences
 
{% endtest %}