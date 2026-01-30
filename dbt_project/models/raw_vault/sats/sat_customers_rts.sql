{{ config(
    materialized='incremental',
    post_hook = "{{ rts_post_hook('CUSTOMER_PK', 'LOAD_DATE') }}" ,
    tags = ["sat","dv2"]  
) }}

{% set source_model = "stage_customers_wrapper" %}
{% set src_pk = "CUSTOMER_PK" %}
{% set src_hashdiff = "CUSTOMER_HASH_DIFF" %}
{# {% set src_payload = ["CUSTOMER_NAME","CUSTOMER_ADDRESS","CUSTOMER_NATION_KEY","CUSTOMER_PHONE"
, "CUSTOMER_ACCTBAL","CUSTOMER_MKTSEGMENT"] %} #}
{% set src_ldts = "LOAD_DATE" %}
{% set src_source = "RECORD_SOURCE" %}

{{ generate_rts(
    source_model=source_model,
    src_pk=src_pk,
    src_hashdiff=src_hashdiff,
    src_ldts=src_ldts,
    src_source=src_source
)}}
