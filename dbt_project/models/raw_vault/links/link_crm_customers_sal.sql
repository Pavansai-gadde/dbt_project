{{ config(materialized='incremental',
tags = ["link","dv2"] ) }}

{% set source_model = "stage_crm_customers" %}
{% set left_hk_col = "left_hk" %}
{% set right_hk_col = "right_hk" %}
{% set src_ldts = "LOAD_DATE" %}
{% set src_source = "RECORD_SOURCE" %}

{{ generate_sal(source_model,
    left_hk_col,
    right_hk_col,
    src_ldts,
    src_source
)}}
