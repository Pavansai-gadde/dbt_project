{{ config(materialized='view') }}
select * from {{ source('landing', 'landing_orders') }}