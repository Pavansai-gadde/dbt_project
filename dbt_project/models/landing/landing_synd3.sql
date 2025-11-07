with source_data as (
    select 8 as id, 'item8' as item,200 as unit_price, '2024-01-03' as fivetran_timestamp union all
    select 9 as id, 'item9' as item,300 as unit_price, '2024-01-03' as fivetran_timestamp union all
    select 10 as id, 'item10' as item,400 as unit_price, '2024-01-03' as fivetran_timestamp union all
    select 4 as id, 'item4' as item,500 as unit_price, '2024-01-03' as fivetran_timestamp union all  
    select 5 as id, 'item5' as item,600 as unit_price, '2024-01-03' as fivetran_timestamp 
)
select * from source_data