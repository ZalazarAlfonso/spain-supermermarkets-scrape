with source_data as (

    select * 
    from {{ source('bronze', 'bronze_carrefour_products_p') }}

),

cleaned as (

    select
        'carrefour' as supermarket,
        cast(product_url as string) as product_url,
        trim(cast(product as string)) as product_name,
        trim(cast(category as string)) as category_raw,
        trim(cast(subcategory as string)) as subcategory_raw,
        trim(cast(brand as string)) as brand_raw,

        cast(price as string) as price_raw,
        cast(price_per_unit as string) as price_per_unit_raw,

        cast(offer as bool) as offer_flag,
        cast(date as date) as scrape_date

    from source_data

),

final as (

    select
        supermarket,
        product_url,
        product_name,
        category_raw,
        subcategory_raw,
        brand_raw,
        price_raw,
        price_per_unit_raw,
        offer_flag,
        scrape_date,

        to_hex(md5(concat(supermarket, '||', product_url))) as source_product_id,
        to_hex(md5(concat(supermarket, '||', product_url, '||', cast(scrape_date as string)))) as product_snapshot_id

    from cleaned

)

select *
from final