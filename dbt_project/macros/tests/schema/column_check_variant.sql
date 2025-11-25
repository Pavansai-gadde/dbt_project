{% test column_check_variant(model, compare_model) %}

with landing_sales as 
    (select distinct upper(f.key) as src_columns
    from {{ compare_model }},
    lateral flatten(input => data) f 
    where f.key is not null  ), 

raw_sales as
 (select column_name as raw_columns
    from {{ model.database }}.information_schema.columns
    where table_schema= '{{ model.schema.upper() }}'
    and table_name = '{{ model.identifier.upper() }}'
    
)

select src_columns
from landing_sales l
left join raw_sales r
  on l.src_columns = r.raw_columns
where r.raw_columns is null


{% endtest %}