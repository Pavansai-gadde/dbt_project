{{ config(materialized='view',
tags = ["stage","dv"]) }}

{# Creating a staging view for customers #}

{% set yaml_metadata %}
source_model: raw_customers_dv
derived_columns:
    RECORD_SOURCE: "!TPCH-CUSTOMERS"
    LOAD_DATE: "current_timestamp()"
    EFFECTIVE_FROM: "current_timestamp()"
    BUSINESS_DTS: created_at
hashed_columns:
    CUSTOMER_PK: 'CUSTOMER_KEY'
    CUSTOMER_HASH_DIFF:
        is_hash_diff: true
        columns:
            - 'CUSTOMER_KEY'
            - 'CUSTOMER_NAME'
            - 'CUSTOMER_ADDRESS'
            - 'CUSTOMER_NATION_KEY'
            - 'CUSTOMER_PHONE'
            - 'CUSTOMER_ACCTBAL'
            - 'CUSTOMER_MKTSEGMENT'
{% endset %}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}
{% set derived_columns = metadata_dict['derived_columns'] %}
{% set hashed_columns = metadata_dict['hashed_columns'] %}

{{ automate_dv.stage(
    include_source_columns=True,
    source_model=source_model,
    derived_columns=derived_columns,
    hashed_columns=hashed_columns
)}}