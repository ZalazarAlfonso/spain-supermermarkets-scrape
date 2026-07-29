with taxonomy_targets as (
    select 'category' as level, category_std as target
    from {{ ref('standard_taxonomy_v1') }}
    union distinct
    select 'subcategory' as level, subcategory_std as target
    from {{ ref('standard_taxonomy_v1') }}
),
rule_targets as (
    select distinct level, target
    from {{ ref('standard_rules_v1') }}
    where level in ('category', 'subcategory')
)

select
    r.level,
    r.target
from rule_targets r
left join taxonomy_targets t
    on r.level = t.level
    and r.target = t.target
where t.target is null
