{{ config (materialized='view',
tags = ["stage","dv"]) }}
{# Creating a staging wrapper for customers #}

select * from {{ ref('stage_customers') }}
qualify row_number() over(
    partition by CUSTOMER_KEY
    order by BUSINESS_DTS desc
)=1