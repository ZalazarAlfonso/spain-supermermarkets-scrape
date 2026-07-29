select
    product_snapshot_id,
    supermarket,
    product_name,
    brand_parse_status,
    brand_std
from {{ ref('int_product_brand_standardized') }}
where brand_parse_status not in ('missing', 'confirmed_no_brand')
  and (brand_std is null or trim(brand_std) = '')
