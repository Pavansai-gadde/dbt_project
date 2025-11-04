{{ config(materialized='view') }}

select * from {{ source('source', 'sampledata_2') }}