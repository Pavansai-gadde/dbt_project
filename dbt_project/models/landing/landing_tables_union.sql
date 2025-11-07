{{ config(
    materialized='ephemeral'
) }}

{{ landing_tables_union(
    landing_models=[
        'landing_synd1',
        'landing_synd2',
        'landing_synd3'
    ],
    unique_key='ID',
    order_by_col='FIVETRAN_TIMESTAMP'
) }}