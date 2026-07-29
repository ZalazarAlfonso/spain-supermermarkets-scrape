select
    p.source_product_id,
    p.supermarket,
    p.brand_std,
    p.brand_parse_status,
    o.brand_std as expected_brand_std,
    o.is_no_brand
from {{ ref('int_product_brand_standardized') }} p
join {{ ref('brand_product_overrides_v1') }} o
  on p.source_product_id = o.source_product_id
 and p.supermarket = o.supermarket
where (o.is_no_brand and (p.brand_std is not null or p.brand_parse_status != 'confirmed_no_brand'))
   or (
       not o.is_no_brand
       and (p.brand_std != o.brand_std or p.brand_parse_status != 'product_override')
   )
