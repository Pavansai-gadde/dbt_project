{{ config(materialized='view') }}

{# Creating a staging view for customers #}

{% set yaml_metadata %}
source_model: raw_orders_dv
derived_columns:
    RECORD_SOURCE: "!TPCH-ORDERS"
    LOAD_DATE: DATEADD(DAY,30,ORDER_DATE)
    EFFECTIVE_FROM: ORDER_DATE 
hashed_columns:
    ORDER_PK: 'ORDER_KEY'
    CUSTOMER_PK: 'ORDER_CUSTOMER_KEY'
    ORDER_CUSTOMER_PK:
        - ORDER_CUSTOMER_KEY
        - ORDER_KEY
    ORDER_HASH_DIFF:
        is_hash_diff: true 
        columns:
            - 'ORDER_KEY'
            - 'ORDER_CUSTOMER_KEY'
            - 'ORDER_STATUS'
            - 'ORDER_TOTAL_PRICE'
            - 'ORDER_DATE'
            - 'ORDER_PRIORITY'
            - 'ORDER_CLERK'
            - 'ORDER_SHIP_PRIORITY'
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