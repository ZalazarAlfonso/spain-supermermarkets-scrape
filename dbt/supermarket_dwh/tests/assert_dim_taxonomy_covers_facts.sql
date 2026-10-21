-- Every taxonomy node a current product carries is a node in dim_taxonomy, so
-- API filters built from dim_taxonomy reach every product.
select distinct
    f.group_std,
    f.category_std,
    f.subcategory_std
from {{ ref('fact_products_today') }} f
left join {{ ref('dim_taxonomy') }} t
    on f.group_std = t.group_std
    and f.category_std = t.category_std
    and f.subcategory_std = t.subcategory_std
where t.group_std is null
