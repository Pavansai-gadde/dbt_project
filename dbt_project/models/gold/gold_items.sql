with dedup as (
select id,item_name,update_date,row_number() over (partition by id order by update_date desc) as rn
from {{ source('source','product_items') }}
)

select id,item_name,update_date
from dedup
where rn = 1