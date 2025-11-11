{% test column_drift_detection(model) %}

with current_cols as (
    select column_name, data_type
    from {{ model.database }}.information_schema.columns
    where table_schema = upper('{{ model.schema }}')
      and table_name   = upper('{{ model.identifier }}')
),

previous_cols as (
    select column_name, data_type
    from {{ ref('column_drift_baseline') }}
)

select 
    coalesce(c.column_name, p.column_name) as column_name,
    p.data_type as baseline_type,
    c.data_type as current_type
from previous_cols p
full outer join current_cols c
    on p.column_name = c.column_name
where p.data_type <> c.data_type
   or p.column_name is null
   or c.column_name is null

{% endtest %}
