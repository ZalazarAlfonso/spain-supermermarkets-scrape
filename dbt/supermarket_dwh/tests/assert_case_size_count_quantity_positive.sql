select
    product_snapshot_id,
    product_name,
    case_count_measure,
    case_count_quantity
from {{ ref('int_product_case_size_parsed') }}
where case_count_quantity is not null
  and case_count_quantity <= 0
