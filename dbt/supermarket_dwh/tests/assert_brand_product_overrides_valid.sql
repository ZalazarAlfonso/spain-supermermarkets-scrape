select
    source_product_id,
    supermarket,
    brand_std,
    is_no_brand
from {{ ref('brand_product_overrides_v1') }}
where (is_no_brand and nullif(trim(brand_std), '') is not null)
   or (not is_no_brand and nullif(trim(brand_std), '') is null)
