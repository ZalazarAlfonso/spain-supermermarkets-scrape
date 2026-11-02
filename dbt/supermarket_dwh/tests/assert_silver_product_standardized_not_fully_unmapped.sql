{{ config(severity='warn') }}

-- Partial mappings may temporarily retain an Other subcategory, but a product
-- should not leave silver with its entire taxonomy unresolved. Warn-only: one
-- new source node must not block the gold build. The share test
-- assert_silver_product_standardized_unmapped_share is the hard limit, and
-- qa_unmapped_products lists what needs a rule.
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
