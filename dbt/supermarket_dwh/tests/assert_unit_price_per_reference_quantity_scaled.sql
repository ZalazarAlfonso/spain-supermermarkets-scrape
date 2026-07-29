{#
    Sources quote weight/volume unit prices against a reference quantity
    ("8.000 €/100 ml"), not always against the base unit. If the divisor is ever
    dropped, the standardized value silently comes out 100x too high — a wrong
    number is worse than the null it replaced, and nothing else here would catch
    it. Recompute the expected value straight from the raw string.
#}
with per_reference_rows as (
    select
        product_snapshot_id,
        product_name,
        price_per_unit_raw,
        unit_price_value_raw,
        unit_price_divisor,
        unit_base_unit_std,
        unit_price_std_value,
        case
            when unit_base_unit_std in ('g', 'ml')
                then safe_divide(unit_price_value_raw, unit_price_divisor) * 1000
            else safe_divide(unit_price_value_raw, unit_price_divisor)
        end as expected_std_value
    from {{ ref('int_products_prices_parsed') }}
    where unit_price_divisor > 1
      and unit_base_unit_std in ('kg', 'g', 'l', 'ml')
)

select *
from per_reference_rows
where unit_price_std_value is null
   or abs(unit_price_std_value - expected_std_value) > 0.000001
