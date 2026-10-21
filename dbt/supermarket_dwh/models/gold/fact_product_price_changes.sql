-- One row per observed price change: a product's price on a scrape date
-- against its previous observation. Pairs that straddle Mercadona's switch
-- from bulk price to shelf price are left out, since the "change" there is a
-- change of meaning, not of price.
with snapshots as (
    select
        source_product_id,
        supermarket,
        product_name,
        group_std,
        category_std,
        subcategory_std,
        scrape_date,
        price_value,
        offer_flag,
        price_is_shelf_price,
        is_price_comparable
    from {{ ref('fact_product_price_snapshots') }}
),
paired as (
    select
        *,
        lag(scrape_date) over w as previous_scrape_date,
        lag(price_value) over w as previous_price_value,
        lag(offer_flag) over w as previous_offer_flag,
        lag(price_is_shelf_price) over w as previous_price_is_shelf_price
    from snapshots
    window w as (partition by source_product_id order by scrape_date)
)

select
    source_product_id,
    supermarket,
    product_name,
    group_std,
    category_std,
    subcategory_std,
    scrape_date,
    previous_scrape_date,
    date_diff(scrape_date, previous_scrape_date, day) as days_since_previous,
    previous_price_value,
    price_value,
    price_value - previous_price_value as price_change_value,
    safe_divide(price_value - previous_price_value, previous_price_value) as price_change_pct,
    if(price_value > previous_price_value, 'up', 'down') as direction,
    previous_offer_flag,
    offer_flag,
    price_is_shelf_price,
    is_price_comparable,
    -- Moves this large are more often a parsing or listing error than a real
    -- price change; keep them, but let the API and alerts filter them.
    abs(safe_divide(price_value - previous_price_value, previous_price_value)) > 0.5 as is_suspect_jump
from paired
where previous_price_value is not null
  and price_value != previous_price_value
  and price_is_shelf_price = previous_price_is_shelf_price
