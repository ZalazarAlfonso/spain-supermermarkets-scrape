select
    category_std,
    subcategory_std
from {{ ref('standard_taxonomy_v1') }}
group by
    category_std,
    subcategory_std
having count(*) > 1
