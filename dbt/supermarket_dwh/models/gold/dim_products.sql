with base as (
    select *
    from {{ ref('silver_product_standardized') }}
),
latest_product_per_source_id as (
    select
        *,
        ROW_NUMBER() OVER (
            PARTITION BY source_product_id
            ORDER BY scrape_date DESC, product_url
        ) as rn
    from base
)

select
    source_product_id,
    supermarket,
    product_url,
    product_name,
    group_std,
    category_std,
    subcategory_std,
    brand_raw
FROM latest_product_per_source_id
WHERE rn = 1
