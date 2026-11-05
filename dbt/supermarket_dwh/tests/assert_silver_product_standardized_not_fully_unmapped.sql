-- Partial mappings may temporarily retain an Other subcategory, but a product
-- must not leave silver with its entire taxonomy unresolved.
--
-- Warns on any unmapped product, fails only above 500 (~5% of one day's ~10k
-- rows). As a hard error on every row it froze gold from 2026-07-29 to
-- 2026-11-04: a handful of new products a day (0.1-0.5%) blocked every gold
-- model, and the app compared baskets on three-month-old prices. Unmapped rows
-- still reach gold as 'Other', so prices stay correct; only the category is
-- missing. Latest day only: silver is incremental, so a new rule never fixes
-- old partitions, and an all-history check could never pass again.
{{ config(warn_if='>0', error_if='>500') }}

select
    product_snapshot_id,
    supermarket,
    category_norm,
    subcategory_norm,
    product_name
from {{ ref('silver_product_standardized') }}
where group_std = 'Other'
  and category_std = 'Other'
  and subcategory_std = 'Other'
  and scrape_date = (select max(scrape_date) from {{ ref('silver_product_standardized') }})
