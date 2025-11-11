{% test datatype_match(model, compare_model) %}

with src as (
    select column_name, data_type
    from {{ compare_model.database }}.information_schema.columns
    where table_schema = upper('{{ compare_model.schema }}')
      and table_name   = upper('{{ compare_model.identifier }}')
),
tgt as (
    select column_name, data_type
    from {{ model.database }}.information_schema.columns
    where table_schema = upper('{{ model.schema }}')
      and table_name   = upper('{{ model.identifier }}')
)

select 
    coalesce(s.column_name, t.column_name) as column_name,
    s.data_type as src_type,
    t.data_type as tgt_type
from src s
full outer join tgt t 
    on s.column_name = t.column_name
where s.data_type <> t.data_type
   or s.data_type is null
   or t.data_type is null

{% endtest %}
