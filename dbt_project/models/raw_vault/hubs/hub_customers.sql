{{ config(materialized='incremental'
,tags = ["hub","dv"]) }}

{% set source_model = "stage_customers_wrapper" %}
{% set src_pk = "CUSTOMER_PK" %}
{% set src_nk = "CUSTOMER_KEY" %}
{% set src_ldts = "LOAD_DATE" %}
{% set src_source = "RECORD_SOURCE" %}

{{ automate_dv.hub(
    source_model=source_model,
    src_pk=src_pk,
    src_nk=src_nk,
    src_ldts=src_ldts,
    src_source=src_source
)}}
