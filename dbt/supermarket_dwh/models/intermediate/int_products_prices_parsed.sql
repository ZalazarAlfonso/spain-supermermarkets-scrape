{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={"field": "scrape_date", "data_type": "date"},
    cluster_by=["supermarket"],
    on_schema_change='sync_all_columns'
) }}

{#
    Unit-price shapes seen across the four sources. The separator is optional
    because Alcampo writes "0,14 € Unidad" with neither "/" nor "por", and the
    quantity before the unit matters because Mercadona and Carrefour quote
    "8.000 €/100 ml" rather than a per-litre price.

    Every unit alternation is `\b`-prefixed on purpose: without it, `(l)\b`
    matches the "l" inside "ml" and every per-100-ml price would be read as a
    per-litre one.
#}
{% set u_ml     = '\\b(ml|mililitros?)\\b' %}
{% set u_l      = '\\b(l|litros?)\\b' %}
{% set u_kg     = '\\b(kg|kilos?|kilogramos?)\\b' %}
{% set u_g      = '\\b(g|gr|gramos?)\\b' %}
{% set u_ud     = '\\b(ud|uds|unidad|unidades)\\b' %}
{% set u_lavado = '\\b(lavados?|lv)\\b' %}
{% set u_docena = '\\b(docenas?|dc|dz)\\b' %}
{% set u_m      = '\\b(m|metros?)\\b' %}

with base as (

    select *
    from {{ ref('int_products_union') }}

),

normalized as (

    select
        *,
        trim(replace(replace(price_raw, '€', ''), ',', '.')) as price_raw_norm,
        {#
            Split digit-letter runs so "100ml" becomes "100 ml". The unit
            patterns below are `\b`-anchored, and a digit is a word character,
            so without this the compact form never matches its unit.
        #}
        regexp_replace(
            lower(trim(replace(price_per_unit_raw, ',', '.'))),
            r'(\d)([a-z])',
            r'\1 \2'
        ) as price_per_unit_raw_norm
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

        {#
            The reference quantity the price is quoted against: 100 in
            "8.000 €/100 ml", implicitly 1 in "2,50 €/kg". Applying this is not
            optional — without it a per-100-ml price is wrong by 100x, which is
            worse than the null it replaces.
        #}
        coalesce(
            safe_cast(
                regexp_extract(
                    price_per_unit_raw_norm,
                    r'(?:/|\bpor\b)\s*(\d+(?:\.\d+)?)\s*(?:kg|kilos?|kilogramos?|g|gr|gramos?|ml|mililitros?|l|litros?)\b'
                ) as numeric
            ),
            1
        ) as unit_price_divisor,

        case
            when regexp_contains(price_per_unit_raw_norm, r'{{ u_ml }}')     then 'ml'
            when regexp_contains(price_per_unit_raw_norm, r'{{ u_l }}')      then 'l'
            when regexp_contains(price_per_unit_raw_norm, r'{{ u_kg }}')     then 'kg'
            when regexp_contains(price_per_unit_raw_norm, r'{{ u_g }}')      then 'g'
            when regexp_contains(price_per_unit_raw_norm, r'{{ u_lavado }}') then 'lavado'
            when regexp_contains(price_per_unit_raw_norm, r'{{ u_docena }}') then 'docena'
            when regexp_contains(price_per_unit_raw_norm, r'{{ u_m }}')      then 'm'
            when regexp_contains(price_per_unit_raw_norm, r'{{ u_ud }}')     then 'ud'
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
            {# A wash and a dozen are both counts of sellable units, so they
               share the 'ud' axis the product-name parser already produces. #}
            when unit_base_unit_std in ('ud', 'lavado', 'docena') then 'count'
            when unit_base_unit_std = 'm' then 'length'
            else null
        end as unit_family,

        case
            when unit_base_unit_std in ('kg', 'l', 'ud', 'lavado', 'm')
                then safe_divide(unit_price_value_raw, unit_price_divisor)
            when unit_base_unit_std in ('g', 'ml')
                then safe_divide(unit_price_value_raw, unit_price_divisor) * 1000
            when unit_base_unit_std = 'docena'
                then safe_divide(unit_price_value_raw, unit_price_divisor * 12)
            else null
        end as unit_price_std_value,

        case
            when unit_base_unit_std in ('kg', 'g') then 'kg'
            when unit_base_unit_std in ('l', 'ml') then 'l'
            when unit_base_unit_std in ('ud', 'lavado', 'docena') then 'ud'
            when unit_base_unit_std = 'm' then 'm'
            else null
        end as unit_price_std_unit

    from parsed

)

select *
from final
{{ incremental_scrape_date_filter() }}
