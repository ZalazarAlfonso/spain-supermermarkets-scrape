with base as (
    select *
    from {{ref('int_products_quality_audit')}}
),
filtered as (
    select *
    from base
    where quality_status not in (
        "drop_missing_product_url",
        "drop_missing_product_name",
        "drop_missing_price_raw",
        "drop_unparsed_price"
    )
),
final as (
    select
        product_snapshot_id,
        source_product_id,
        supermarket,
        scrape_date,

        product_url,
        product_name,

        category_raw,
        subcategory_raw,
        brand_raw,

        price_raw,
        price_value,

        price_per_unit_raw,
        price_per_unit_raw_norm,
        unit_price_value_raw,
        unit_base_unit_std,
        unit_family,
        unit_price_std_value,
        unit_price_std_unit,

        offer_flag,
        quality_status
    from
        filtered
)

select *
from final