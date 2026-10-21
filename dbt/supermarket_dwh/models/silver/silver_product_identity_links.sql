-- Links a product ID that disappeared to the ID that replaced it the next few
-- days, when the supermarket changed the product's URL. source_product_id is a
-- hash of the URL, so without these links a URL change starts a new price
-- history.
--
-- A link needs: same supermarket, same normalized name, same pack size, the
-- new ID first seen 1-3 days after the old one was last seen, and no other
-- candidate on either side. Names are reused for different products, so this
-- stays conservative: an ambiguous pair is left unlinked.
with products as (
    select
        source_product_id,
        supermarket,
        any_value(product_norm having max scrape_date) as name_key,
        any_value(format('%t|%t', case_total_quantity, case_total_unit) having max scrape_date) as size_key,
        min(scrape_date) as first_seen_date,
        max(scrape_date) as last_seen_date
    from {{ ref('silver_product_standardized') }}
    group by source_product_id, supermarket
),
candidates as (
    select
        o.source_product_id as previous_product_id,
        n.source_product_id as next_product_id,
        o.supermarket,
        o.name_key,
        o.size_key,
        o.last_seen_date as previous_last_seen_date,
        n.first_seen_date as next_first_seen_date
    from products o
    join products n
        on o.supermarket = n.supermarket
        and o.name_key = n.name_key
        and o.size_key = n.size_key
        and o.source_product_id != n.source_product_id
        and n.first_seen_date between date_add(o.last_seen_date, interval 1 day)
                                  and date_add(o.last_seen_date, interval {{ var('identity_max_gap_days') }} day)
)

select *
from candidates
qualify count(*) over (partition by previous_product_id) = 1
    and count(*) over (partition by next_product_id) = 1
