{{ config(materialized='view') }}

-- Source navigation nodes that no taxonomy rule places, after the per-product
-- fallback. Each row is a candidate for a new rule in standard_rules_v1.
select
    supermarket,
    category_norm,
    subcategory_norm,
    count(*) as snapshot_rows,
    count(distinct source_product_id) as products,
    min(scrape_date) as first_scrape_date,
    max(scrape_date) as last_scrape_date,
    any_value(product_name) as example_product_name
from {{ ref('silver_product_standardized') }}
where taxonomy_method = 'unmapped'
group by supermarket, category_norm, subcategory_norm
