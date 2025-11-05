with sales as (
    select "sales_id",
    "store_sk",
    "product_sk",
    "customer_sk",
    "gross_amount"
    from {{ ref('bronze_sales') }}
),
products as (
    select "product_sk",
    "product_name",
    "category"
    from {{ ref("bronze_product") }}
),
customers as (
    select "customer_sk",
    "gender",
    "loyalty_tier"
    from {{ ref("bronze_customer") }}
)
,joined_data as (
    select s."sales_id",
    p."product_name",
    p."category",
    c."gender",
    c."loyalty_tier",
    s."gross_amount"
    from sales as s 
    inner join products as p 
    on s."product_sk" = p."product_sk"
    inner join customers as c 
    on s."customer_sk" = c."customer_sk"
)

select "gender",
"category",
sum("gross_amount") as total_gross_amount
from joined_data 
group by all