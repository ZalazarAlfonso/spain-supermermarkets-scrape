-- Subcategory labels are used to resolve their canonical category and group.
-- A label must therefore have exactly one parent in the taxonomy.
select
    subcategory_std
from {{ ref('standard_taxonomy_v1') }}
group by subcategory_std
having count(distinct concat(group_std, '||', category_std)) > 1
