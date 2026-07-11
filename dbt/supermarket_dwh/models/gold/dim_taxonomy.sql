with base as (
    select
        *
    from {{ ref('standard_taxonomy_v1') }}
),
distinct_values as (
    select
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                group_std, category_std, subcategory_std 
                ORDER BY group_order, category_order, subcategory_order
         ) as rn
    from base
)
select
    group_order,
    category_order,
    subcategory_order,
    group_std as product_group,
    category_std as category,
    subcategory_std as subcategory
from distinct_values
where rn = 1