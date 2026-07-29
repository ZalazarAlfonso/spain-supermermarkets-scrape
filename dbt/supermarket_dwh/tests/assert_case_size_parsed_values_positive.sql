select
    product_snapshot_id,
    product_name,
    case_pack_count,
    case_unit_quantity,
    case_total_quantity
from {{ ref('int_product_case_size_parsed') }}
where case_parse_status = 'parsed'
  and (
      case_pack_count < 1
      or case_total_quantity <= 0
      or (case_unit_measure is not null and case_unit_quantity <= 0)
  )
