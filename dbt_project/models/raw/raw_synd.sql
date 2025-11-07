with landing_synd_union as (
    select id,
    item,
    unit_price,
    fivetran_timestamp
    from {{ ref('landing_synd1') }}
    union all
    select id,
    item,
    unit_price,
    fivetran_timestamp
    from {{ ref('landing_synd2') }}
    union all 
    select id,
    item,
    unit_price,
    fivetran_timestamp
    from {{ ref('landing_synd3') }}        
)
select id,
       item,
       unit_price
       from (
select  *,
    row_number() over(partition by id order by fivetran_timestamp desc ) as rn 
    from landing_synd_union
       )
where rn=1