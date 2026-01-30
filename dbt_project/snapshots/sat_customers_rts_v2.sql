{% snapshot sat_customers_rts_v2 %}

{{ config(
    target_schema='data_vault',
    unique_key='CUSTOMER_PK',
    strategy='timestamp',
    updated_at='LOAD_DATE'
) 
}}

select CUSTOMER_PK,CUSTOMER_HASH_DIFF,LOAD_DATE,RECORD_SOURCE 
from {{ ref('stage_customers_wrapper') }}

{% endsnapshot %}