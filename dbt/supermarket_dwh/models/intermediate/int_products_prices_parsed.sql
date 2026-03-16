with base as (

    select *
    from {{ ref('int_products_union') }}

),

normalized as (

    select
        *,
        trim(replace(replace(price_raw, '€', ''), ',', '.')) as price_raw_norm,
        lower(trim(replace(price_per_unit_raw, ',', '.'))) as price_per_unit_raw_norm
    from base

),

parsed as (

    select
        *,

        safe_cast(price_raw_norm as numeric) as price_value,

        safe_cast(
            regexp_extract(price_per_unit_raw_norm, r'(\d+(?:\.\d+)?)')
            as numeric
        ) as unit_price_value_raw,

        case
            when regexp_contains(price_per_unit_raw_norm, r'(?:/|\bpor\b)\s*litro\b')
              or regexp_contains(price_per_unit_raw_norm, r'/l\b')
                then 'l'

            when regexp_contains(price_per_unit_raw_norm, r'/kg\b')
              or regexp_contains(price_per_unit_raw_norm, r'(?:/|\bpor\b)\s*(kg|kilo|kilogramo)\b')
                then 'kg'

            when regexp_contains(price_per_unit_raw_norm, r'/ud\b')
              or regexp_contains(price_per_unit_raw_norm, r'(?:/|\bpor\b)\s*(ud|unidad)\b')
                then 'ud'

            when regexp_contains(price_per_unit_raw_norm, r'/g\b')
              or regexp_contains(price_per_unit_raw_norm, r'(?:/|\bpor\b)\s*(g|gr|gramo)\b')
                then 'g'

            when regexp_contains(price_per_unit_raw_norm, r'/ml\b')
              or regexp_contains(price_per_unit_raw_norm, r'(?:/|\bpor\b)\s*(ml|mililitro)\b')
                then 'ml'

            else null
        end as unit_base_unit_std

    from normalized

),

final as (

    select
        *,

        case
            when unit_base_unit_std in ('kg', 'g') then 'weight'
            when unit_base_unit_std in ('l', 'ml') then 'volume'
            when unit_base_unit_std in ('ud') then 'count'
            else null
        end as unit_family,

        case
            when unit_base_unit_std = 'kg' then unit_price_value_raw
            when unit_base_unit_std = 'l' then unit_price_value_raw
            when unit_base_unit_std = 'ud' then unit_price_value_raw
            when unit_base_unit_std = 'g' then unit_price_value_raw * 1000
            when unit_base_unit_std = 'ml' then unit_price_value_raw * 1000
            else null
        end as unit_price_std_value,

        case
            when unit_base_unit_std in ('kg', 'g') then 'kg'
            when unit_base_unit_std in ('l', 'ml') then 'l'
            when unit_base_unit_std = 'ud' then 'ud'
            else null
        end as unit_price_std_unit

    from parsed

)

select *
from final