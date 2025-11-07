with source_data as (
    select 1 as id, 'item1' as item,200 as unit_price, '2024-01-02' as fivetran_timestamp union all
    select 2 as id, 'item2' as item,300 as unit_price, '2024-01-02' as fivetran_timestamp union all
    select 6 as id, 'item6' as item,400 as unit_price, '2024-01-02' as fivetran_timestamp union all
    select 7 as id, 'item7' as item,500 as unit_price, '2024-01-02' as fivetran_timestamp union all  
    select 8 as id, 'item8' as item,600 as unit_price, '2024-01-02' as fivetran_timestamp 
)
select * from source_data