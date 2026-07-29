select
    brand_match_norm,
    coalesce(supermarket, '') as supermarket_scope
from {{ ref('int_brand_dictionary') }}
group by 1, 2
having count(*) > 1
