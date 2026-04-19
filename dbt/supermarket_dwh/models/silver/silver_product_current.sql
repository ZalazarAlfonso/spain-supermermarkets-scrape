with base as (
    select *
    from {{ref('silver_product_snapshots')}}
),
filtered as (
    select *
    from base
    where scrape_date = (select max(scrape_date) from base)
)
select *
from filtered