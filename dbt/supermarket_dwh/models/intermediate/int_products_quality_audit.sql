with base as (
    select *
    from {{ref('int_products_prices_parsed')}}
),
final as (
    select
        *,
        case
            when product_url is null or trim(product_url) = '' then 'drop_missing_product_url'
            when product_name is null or trim(product_name) = '' then 'drop_missing_product_name'
            when price_raw is null or trim(price_raw) = '' then 'drop_missing_price_raw'
            when price_value is null then 'drop_unparsed_price'
            when price_per_unit_raw is null or trim(price_per_unit_raw) = '' then 'keep_missing_unit_price_raw'
            when unit_price_value_raw is null then 'keep_unparsed_unit_price_value'
            when unit_base_unit_std is null then 'keep_unknown_unit_pattern'
            else 'keep_fully_parsed'
        end as quality_status
    from base
)

select *
from final