{{ config(
    materialized='incremental',
    unique_key='ORDER_CUSTOMER_PK',     
    incremental_strategy='merge'  ,
    tags = ["link","dv2"]   
) }}


{% set source_model = "stage_orders" %}
{% set src_pk = "ORDER_CUSTOMER_PK" %}
{% set src_fk = ["ORDER_PK", "CUSTOMER_PK"] %}
{% set src_ldts = "LOAD_DATE" %}
{% set src_source = "RECORD_SOURCE" %}

{{ generate_nhl(
    source_model=source_model,
    src_pk=src_pk,
    src_fk=src_fk,
    src_ldts=src_ldts,
    src_source=src_source
)}} 
