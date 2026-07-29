select
    brand_alias_norm,
    coalesce(supermarket, '') as supermarket_scope
from {{ ref('brand_aliases_v1') }}
group by 1, 2
having count(*) > 1
