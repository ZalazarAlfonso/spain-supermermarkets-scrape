{{ config(materialized='view') }}

-- A stable random sample of identity links with both URLs and the prices on
-- either side of the switch, for checking by hand before the links feed
-- price history.
select
    l.supermarket,
    l.name_key,
    l.size_key,
    l.previous_last_seen_date,
    l.next_first_seen_date,
    prev.product_url as previous_url,
    next.product_url as next_url,
    prev.price_value as previous_price_value,
    next.price_value as next_price_value
from {{ ref('silver_product_identity_links') }} l
join {{ ref('silver_product_standardized') }} prev
    on prev.source_product_id = l.previous_product_id
    and prev.scrape_date = l.previous_last_seen_date
join {{ ref('silver_product_standardized') }} next
    on next.source_product_id = l.next_product_id
    and next.scrape_date = l.next_first_seen_date
order by farm_fingerprint(l.previous_product_id)
limit 200
