select
    product_snapshot_id,
    product_name,
    case_pack_count
from {{ ref('int_product_case_size_parsed') }}
where case_type = 'multipack'
  and (case_pack_count is null or case_pack_count <= 1)
