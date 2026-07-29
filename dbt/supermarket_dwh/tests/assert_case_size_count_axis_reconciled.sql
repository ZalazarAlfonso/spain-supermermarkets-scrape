{#
    A product name that states a unit count is not in conflict with a per-kilo or
    per-litre shelf price: the pack has both a count and a weight. Whenever the
    store publishes a usable unit price, the measurable axis must be derived from
    it rather than falling through to `incompatible_units`.
#}
select
    product_snapshot_id,
    product_name,
    case_count_measure,
    case_count_quantity,
    case_total_unit,
    unit_price_std_unit,
    case_validation_status
from {{ ref('int_product_case_size_parsed') }}
where case_validation_status = 'incompatible_units'
  and case_count_measure is not null
  {# A name that also states grams or millilitres carries its own measurable
     axis; if that one disagrees with the shelf unit price it is a real conflict
     (weight vs volume needs a density we do not have) and must stay flagged. #}
  and case_unit_measure is null
  and unit_price_std_value > 0
  and unit_price_std_unit in ('kg', 'l')
