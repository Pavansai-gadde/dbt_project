{{ config(
    materialized='incremental',
    unique_key='CUSTOMER_PK',     
    incremental_strategy='merge', 
    tags = ["sat","dv2"]   
) }} 

{% set source_model = "stage_customers_wrapper" %}
{% set src_pk = "CUSTOMER_PK" %}
{# {% set src_hashdiff = {"source_column":"CUSTOMER_HASH_DIFF",
                        "alias":"HASH_DIFF"}%} #}
{% set src_payload = ["CUSTOMER_NAME","CUSTOMER_ADDRESS","CUSTOMER_NATION_KEY","CUSTOMER_PHONE"
, "CUSTOMER_ACCTBAL","CUSTOMER_MKTSEGMENT"] %}
{% set src_eff = "EFFECTIVE_FROM" %}
{% set src_ldts = "LOAD_DATE" %}
{% set src_source = "RECORD_SOURCE" %}

{{ generate_nhs(
    source_model=source_model,
    src_pk=src_pk,
    src_payload=src_payload,
    src_eff=src_eff,
    src_ldts=src_ldts,
    src_source=src_source
)}}
