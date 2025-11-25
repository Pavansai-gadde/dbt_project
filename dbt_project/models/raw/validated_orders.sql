{{ config (materialized='table') }}

{{ validate_rules('raw_orders')  }}