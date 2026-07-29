-- Partial mappings may temporarily retain an Other subcategory, but a product
-- must not leave silver with its entire taxonomy unresolved.
select
    product_snapshot_id,
    supermarket,
    category_norm,
    subcategory_norm,
    product_name
from {{ ref('silver_product_standardized') }}
where group_std = 'Other'
  and category_std = 'Other'
  and subcategory_std = 'Other'
