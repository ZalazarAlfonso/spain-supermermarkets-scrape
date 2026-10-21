-- One completeness row per supermarket and scrape date.
select supermarket, scrape_date, count(*) as n
from {{ ref('silver_supermarket_daily_completeness') }}
group by supermarket, scrape_date
having count(*) > 1
