select
    p.source_product_id,
    p.supermarket,
    p.brand_std,
    p.brand_parse_status,
    r.brand_inferred,
    r.brand_link_review_status
from {{ ref('int_product_brand_standardized') }} p
join {{ ref('brand_reviewed_products_v1') }} r
  on p.source_product_id = r.source_product_id
 and p.supermarket = r.supermarket
where r.brand_link_review_status != 'not_in_low_medium_scope'
  and (
      (lower(trim(r.brand_inferred)) = 'sin marca'
       and (p.brand_std is not null or p.brand_parse_status != 'confirmed_no_brand'))
      or (lower(trim(r.brand_inferred)) != 'sin marca'
          and (p.brand_std != trim(r.brand_inferred) or p.brand_parse_status != 'product_override'))
  )
