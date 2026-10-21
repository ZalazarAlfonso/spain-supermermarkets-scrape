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
        ) as rn,
        MIN(scrape_date) OVER (PARTITION BY source_product_id) as first_seen_date,
        COUNT(*) OVER (PARTITION BY source_product_id) as days_observed
    from base
),
-- The date each supermarket's current catalogue is taken from; a product seen
-- on it is still listed.
supermarket_as_of as (
    select
        supermarket,
        max(case when is_complete then scrape_date end) as as_of_date
    from {{ ref('silver_supermarket_daily_completeness') }}
    group by supermarket
)

select
    p.source_product_id,
    p.supermarket,
    product_url,
    product_name,
    {{ product_search_text() }} as search_text,
    group_std,
    category_std,
    subcategory_std,
    brand_raw,
    brand_norm,
    brand_std,
    brand_group,
    is_private_label,
    brand_parse_status,
    brand_parse_method,
    case_size_raw,
    case_pack_count,
    case_unit_quantity,
    case_unit_measure,
    case_count_measure,
    case_count_quantity,
    case_total_quantity,
    case_total_unit,
    case_unit_family,
    case_type,
    case_parse_status,
    case_parse_method,
    case_validation_status,
    first_seen_date,
    scrape_date as last_seen_date,
    days_observed,
    scrape_date >= a.as_of_date as is_active
FROM latest_product_per_source_id p
LEFT JOIN supermarket_as_of a
    ON p.supermarket = a.supermarket
WHERE rn = 1
