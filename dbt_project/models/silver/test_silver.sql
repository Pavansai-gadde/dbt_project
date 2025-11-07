{# select {{ generate_column_names('bronze_customer') }} from ref('bronze_customer') #}


{# select count(*) from (
{{ landing_tables_union(['bronze_customer', 'bronze_product'],'customer_id','fiveran_timestamp') }}
) #}