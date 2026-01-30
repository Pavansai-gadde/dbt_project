{{ config (materialized='view') }} 

with source_data as(
    select customer_id,
    billing_customer_id
    from {{ source('data_vault_src','crm_customers') }}
) 
select least({{ dbt_utils.generate_surrogate_key(['customer_id']) }},
{{ dbt_utils.generate_surrogate_key(['billing_customer_id']) }}) as left_hk,
greatest({{ dbt_utils.generate_surrogate_key(['customer_id']) }},
{{ dbt_utils.generate_surrogate_key(['billing_customer_id']) }}) as right_hk,
current_timestamp() as load_date,
'CRM_CUSTOMERS' as record_source
from source_data  
where customer_id <> billing_customer_id