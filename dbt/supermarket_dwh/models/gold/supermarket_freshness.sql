-- Per-supermarket data freshness for the API's health endpoint and the
-- dashboard: what the latest scrape returned, and which date the current
-- catalogue (fact_products_today) is taken from.
with completeness as (
    select *
    from {{ ref('silver_supermarket_daily_completeness') }}
),
latest as (
    select *
    from completeness
    qualify row_number() over (partition by supermarket order by scrape_date desc) = 1
),
latest_complete as (
    select *
    from completeness
    where is_complete
    qualify row_number() over (partition by supermarket order by scrape_date desc) = 1
)

select
    l.supermarket,
    l.scrape_date as latest_scrape_date,
    l.snapshot_rows as latest_scrape_rows,
    l.baseline_rows as latest_baseline_rows,
    l.volume_ratio as latest_volume_ratio,
    l.is_complete as latest_scrape_is_complete,
    c.scrape_date as as_of_date,
    c.snapshot_rows as as_of_rows,
    date_diff(current_date('Europe/Madrid'), c.scrape_date, day) as days_stale,
    date_diff(current_date('Europe/Madrid'), c.scrape_date, day) > {{ var('stale_after_days') }} as is_stale,
    current_timestamp() as built_at
from latest l
left join latest_complete c
    on l.supermarket = c.supermarket
