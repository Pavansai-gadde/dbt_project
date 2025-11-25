{{ config(materialized='view') }}
select * from {{ source('raw', 'raw_sales') }}