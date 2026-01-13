{{ config (materialized='table') }}

with raw_orders as (

    select raw_order_seq.nextval as raw_orders_id,
order_id,
customer_id,
coalesce(try_to_date(order_date,'YYYY-MM-DD'),
try_to_date(order_date,'YYYY/MM/DD'),
try_to_date(order_date,'DD/MM/YYYY')
) as order_date,
try_to_number(amount,38,2) as amount,
current_timestamp() as inserted_at
    from {{ source('landing', 'landing_orders') }}
)

select * from raw_orders 