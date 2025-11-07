with source_data as (
    select 1 as id, 'item1' as item,200 as unit_price, '2024-01-01' as fivetran_timestamp union all
    select 2 as id, 'item2' as item,300 as unit_price, '2024-01-01' as fivetran_timestamp union all
    select 3 as id, 'item3' as item,400 as unit_price, '2024-01-01' as fivetran_timestamp union all
    select 4 as id, 'item4' as item,500 as unit_price, '2024-01-01' as fivetran_timestamp union all  
    select 5 as id, 'item5' as item,600 as unit_price, '2024-01-01' as fivetran_timestamp 
)
select * from source_data