{{ config (materialized='table') }}

with validated as (
    select * from {{ ref('validated_orders') }}
)
select * from validated 
where rule_id is not null