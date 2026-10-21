-- Each supermarket's catalogue as of its own latest complete scrape. Taking one
-- global max(scrape_date) dropped a whole supermarket whenever its scraper was
-- late, and showed a handful of products when its scraper was blocked.
with completeness as (
    select *
    from {{ ref('silver_supermarket_daily_completeness') }}
),
as_of as (
    select
        supermarket,
        max(case when is_complete then scrape_date end) as as_of_date,
        max(scrape_date) as latest_scrape_date
    from completeness
    group by supermarket
)

select
    s.*,
    a.as_of_date,
    a.latest_scrape_date,
    date_diff(current_date('Europe/Madrid'), a.as_of_date, day) as days_stale,
    date_diff(current_date('Europe/Madrid'), a.as_of_date, day) > {{ var('stale_after_days') }} as is_stale
from {{ ref('silver_product_standardized') }} s
join as_of a
    on s.supermarket = a.supermarket
    and s.scrape_date = a.as_of_date
