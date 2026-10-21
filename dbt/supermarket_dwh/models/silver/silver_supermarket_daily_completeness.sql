-- One row per supermarket and scrape date: how many products the scrape
-- returned against that supermarket's usual volume. A blocked or crashed
-- scraper still writes a day, just a small one, so "latest date" alone is not
-- "latest complete catalogue".
with daily as (
    select
        supermarket,
        scrape_date,
        count(*) as snapshot_rows
    from {{ ref('silver_product_standardized') }}
    group by supermarket, scrape_date
),
baseline as (
    select
        d.supermarket,
        d.scrape_date,
        -- 90th percentile of the preceding window, so a run of short days
        -- does not quietly lower the bar for the next one.
        approx_quantiles(h.snapshot_rows, 10)[offset(9)] as baseline_rows
    from daily d
    join daily h
        on h.supermarket = d.supermarket
        and h.scrape_date between date_sub(d.scrape_date, interval {{ var('completeness_baseline_days') }} day)
                              and date_sub(d.scrape_date, interval 1 day)
    group by d.supermarket, d.scrape_date
)

select
    d.supermarket,
    d.scrape_date,
    d.snapshot_rows,
    b.baseline_rows,
    safe_divide(d.snapshot_rows, b.baseline_rows) as volume_ratio,
    -- The first days of a supermarket have no history to compare against.
    b.baseline_rows is null
        or d.snapshot_rows >= {{ var('completeness_min_share') }} * b.baseline_rows as is_complete
from daily d
left join baseline b
    on d.supermarket = b.supermarket
    and d.scrape_date = b.scrape_date
