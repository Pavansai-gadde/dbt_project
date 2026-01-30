{{ config(materialized='view') }}

{# Creating a raw stage view which can be used as source to create hash staging layer #}
with raw_orders as (
select 	O_ORDERKEY as ORDER_KEY
,O_CUSTKEY as ORDER_CUSTOMER_KEY
,O_ORDERSTATUS as ORDER_STATUS
,O_TOTALPRICE as ORDER_TOTAL_PRICE
,O_ORDERDATE as ORDER_DATE
,O_ORDERPRIORITY as ORDER_PRIORITY
,O_CLERK as ORDER_CLERK
,O_SHIPPRIORITY as ORDER_SHIP_PRIORITY
from {{ source('snowflake_sample_data', 'orders') }}
)
select * from raw_orders
