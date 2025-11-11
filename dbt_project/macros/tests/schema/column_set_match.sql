{% test column_set_match(model, compare_model) %}

with compare_cols as (
    select column_name
    from {{ compare_model.database }}.information_schema.columns
    where table_schema = upper('{{ compare_model.schema }}')
      and table_name   = upper('{{ compare_model.identifier }}')
),

target_cols as (
    select column_name
    from {{ model.database }}.information_schema.columns
    where table_schema = upper('{{ model.schema }}')
      and table_name   = upper('{{ model.identifier }}')
)

select column_name
from (
    select column_name from compare_cols
    minus
    select column_name from target_cols
)
union all
select column_name
from (
    select column_name from target_cols
    minus
    select column_name from compare_cols
)

{% endtest %}
