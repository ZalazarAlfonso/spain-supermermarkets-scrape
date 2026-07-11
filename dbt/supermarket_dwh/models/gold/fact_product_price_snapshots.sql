with base as (
    select *
    from {{ ref('silver_product_standardized') }}

)
select
    product_snapshot_id, 
    source_product_id,
    supermarket,
    product_url,
    product_name,
    scrape_date,
    group_std,
    category_std,
    subcategory_std,
    brand_raw,
    price_value,
    unit_base_unit_std,
    unit_family,
    unit_price_std_value,
    unit_price_std_unit,
    offer_flag
FROM base
