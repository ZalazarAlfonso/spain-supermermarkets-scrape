WITH base as (
    SELECT * FROM {{ ref('silver_product_current') }}

)
SELECT 
    source_product_id,
    supermarket,
    product_url,
    product_name,
    category_raw,
    subcategory_raw,
    brand_raw,
    price_value,
    unit_base_unit_std,
    unit_family,
    unit_price_std_value,
    unit_price_std_unit,
    offer_flag
FROM base