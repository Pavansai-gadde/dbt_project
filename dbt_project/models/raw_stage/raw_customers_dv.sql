{{ config(materialized='table') }}

{# Creating a raw stage view which can be used as source to create hash staging layer #}
with raw_customers as (
select 	C_CUSTKEY as CUSTOMER_KEY,
	C_NAME as CUSTOMER_NAME,
	C_ADDRESS as CUSTOMER_ADDRESS,
	C_NATIONKEY as CUSTOMER_NATION_KEY,
	C_PHONE as CUSTOMER_PHONE,
	C_ACCTBAL as CUSTOMER_ACCTBAL,
	C_MKTSEGMENT as CUSTOMER_MKTSEGMENT,
	CURRENT_TIMESTAMP as CREATED_AT
from {{ source('snowflake_sample_data', 'customer') }}
)
select * from raw_customers
where CUSTOMER_MKTSEGMENT ='BUILDING' and CUSTOMER_NATION_KEY=2