select
    source_product_id,
    supermarket,
    brand_inferred,
    brand_link_review_status
from {{ ref('brand_reviewed_products_v1') }}
where brand_link_review_status != 'not_in_low_medium_scope'
  and nullif(trim(brand_inferred), '') is null
