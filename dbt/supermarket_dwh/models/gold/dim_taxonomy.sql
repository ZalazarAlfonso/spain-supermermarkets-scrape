-- Standard category tree for filters. Column names match the facts
-- (group_std, category_std, subcategory_std), and the 'Other' nodes the facts
-- can carry are included so every fact row joins to a node.
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
),
nodes as (
    select
        group_order,
        category_order,
        subcategory_order,
        group_std,
        category_std,
        subcategory_std
    from distinct_values
    where rn = 1
),
-- A product whose category is mapped but whose subcategory is not.
category_other as (
    select
        min(group_order) as group_order,
        min(category_order) as category_order,
        999 as subcategory_order,
        group_std,
        category_std,
        'Other' as subcategory_std
    from nodes
    group by group_std, category_std
),
fully_other as (
    select
        999 as group_order,
        999 as category_order,
        999 as subcategory_order,
        'Other' as group_std,
        'Other' as category_std,
        'Other' as subcategory_std
)

select * from nodes
union all
select * from category_other
union all
select * from fully_other
