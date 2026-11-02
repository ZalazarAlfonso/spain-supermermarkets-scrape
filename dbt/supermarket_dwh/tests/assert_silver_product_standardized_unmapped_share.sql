-- Fails when more than 0.1% of silver rows have no taxonomy at all, which means
-- a supermarket's navigation changed wholesale rather than one node going missing.
with counts as (
    select
        countif(group_std = 'Other' and category_std = 'Other' and subcategory_std = 'Other') as unmapped_rows,
        count(*) as total_rows
    from {{ ref('silver_product_standardized') }}
)

select
    unmapped_rows,
    total_rows,
    safe_divide(unmapped_rows, total_rows) as unmapped_share
from counts
where safe_divide(unmapped_rows, total_rows) > 0.001
