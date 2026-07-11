with products as (
    select *
    from {{ ref('silver_product_standardized') }}
),
taxonomy_categories as (
    select distinct category_std
    from {{ ref('standard_taxonomy_v1') }}
),
taxonomy_subcategories as (
    select distinct subcategory_std
    from {{ ref('standard_taxonomy_v1') }}
),
taxonomy_groups as (
    select distinct group_std
    from {{ ref('standard_taxonomy_v1') }}
)

select
    p.product_snapshot_id,
    p.supermarket,
    p.group_std,
    p.category_std,
    p.subcategory_std
from products p
left join taxonomy_groups g
    on p.group_std = g.group_std
left join taxonomy_categories c
    on p.category_std = c.category_std
left join taxonomy_subcategories s
    on p.subcategory_std = s.subcategory_std
where
    (p.group_std != 'Other' and g.group_std is null)
    or (p.category_std != 'Other' and c.category_std is null)
    or (p.subcategory_std != 'Other' and s.subcategory_std is null)
