{#
    Measurement token alternation. Accepts the `gr` / `grs` spellings used by
    several sources alongside the canonical `g`; normalized back to `g` below.
#}
{% set meas = 'kg|g(?:rs?)?|l|cl|ml' %}
{% set count_words = 'uds?\\.?|unidades?|piezas?|rollos?|lavados?|capsulas?|bolsas?|panales?' %}

{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={"field": "scrape_date", "data_type": "date"},
    cluster_by=["supermarket"],
    on_schema_change='sync_all_columns'
) }}

with base as (
    select *
    from {{ ref('int_products_prices_parsed') }}
),
normalized as (
    select
        *,
        replace(replace({{ normalize_text('product_name') }}, ',', '.'), '×', 'x') as case_text_norm
    from base
),
classified as (
    select
        *,
        regexp_contains(case_text_norm, r'\b(?:al peso|peso variable)\b|\baprox(?:\.|imadamente)?\b') as is_variable_weight,
        regexp_contains(
            case_text_norm,
            r'\b\d+\s*x\s*\d+(?:\.\d+)?\s*(?:{{ meas }})\b'
        ) as is_x_multipack,
        regexp_contains(
            case_text_norm,
            r'\bpack\s+(?:de\s+)?\d+\b.*?\b(?:de|x)\s*\d+(?:\.\d+)?\s*(?:{{ meas }})\b'
        ) as is_pack_multipack,
        regexp_contains(
            case_text_norm,
            r'\b\d+\s*(?:uds?\.?|unidades?|botellas?|briks?|latas?|botes?|paquetes?)\s+(?:de|x)\s*\d+(?:\.\d+)?\s*(?:{{ meas }})\b'
        ) as is_container_multipack
    from normalized
),
extracted as (
    select
        *,
        case
            when is_x_multipack then safe_cast(regexp_extract(case_text_norm, r'\b(\d+)\s*x\s*\d+(?:\.\d+)?\s*(?:{{ meas }})\b') as int64)
            when is_pack_multipack then safe_cast(regexp_extract(case_text_norm, r'\bpack\s+(?:de\s+)?(\d+)\b') as int64)
            when is_container_multipack then safe_cast(regexp_extract(case_text_norm, r'\b(\d+)\s*(?:uds?\.?|unidades?|botellas?|briks?|latas?|botes?|paquetes?)\s+(?:de|x)\s*\d+(?:\.\d+)?\s*(?:{{ meas }})\b') as int64)
            when regexp_contains(case_text_norm, r'\b\d+(?:\.\d+)?\s*(?:{{ count_words }})\b') then 1
            when regexp_contains(case_text_norm, r'\b\d+(?:\.\d+)?\s*(?:{{ meas }})\b') then 1
        end as case_pack_count,
        case
            when is_x_multipack or is_pack_multipack or is_container_multipack then
                safe_cast(regexp_extract(case_text_norm, r'(?:x|de)\s*(\d+(?:\.\d+)?)\s*(?:{{ meas }})\b') as numeric)
            else safe_cast(regexp_extract(case_text_norm, r'\b(\d+(?:\.\d+)?)\s*(?:{{ meas }})\b') as numeric)
        end as case_unit_quantity,
        case
            when regexp_extract(case_text_norm, r'\b\d+(?:\.\d+)?\s*({{ meas }})\b') in ('gr', 'grs') then 'g'
            else regexp_extract(case_text_norm, r'\b\d+(?:\.\d+)?\s*({{ meas }})\b')
        end as case_unit_measure,
        safe_cast(
            regexp_extract(
                case_text_norm,
                r'\b(\d+(?:\.\d+)?)\s*(?:{{ count_words }})\b'
            ) as numeric
        ) as explicit_count,
        regexp_extract(
            case_text_norm,
            r'\b\d+(?:\.\d+)?\s*({{ count_words }})\b'
        ) as case_count_measure
    from classified
),
typed as (
    select
        *,
        case
            when (is_x_multipack or is_pack_multipack or is_container_multipack)
              and case_pack_count > 1 then 'multipack'
            when explicit_count is not null then 'count'
            when case_unit_quantity is not null and is_variable_weight then 'variable_weight'
            when case_unit_quantity is not null then 'single'
            when is_variable_weight then 'variable_weight'
            else 'unknown'
        end as case_type,
        case
            when case_unit_measure in ('kg', 'g') then 'weight'
            when case_unit_measure in ('l', 'cl', 'ml') then 'volume'
            when explicit_count is not null then 'count'
        end as case_unit_family,
        case
            when case_unit_measure in ('kg', 'g') then 'kg'
            when case_unit_measure in ('l', 'cl', 'ml') then 'l'
            when explicit_count is not null then 'ud'
        end as case_total_unit,
        case
            when case_unit_measure = 'kg' then case_pack_count * case_unit_quantity
            when case_unit_measure = 'g' then case_pack_count * case_unit_quantity / 1000
            when case_unit_measure = 'l' then case_pack_count * case_unit_quantity
            when case_unit_measure = 'cl' then case_pack_count * case_unit_quantity / 100
            when case_unit_measure = 'ml' then case_pack_count * case_unit_quantity / 1000
            when explicit_count is not null then explicit_count
        end as case_total_quantity
    from extracted
),
{#
    Count and weight/volume are two independent axes of the same pack. The name
    may state either, both, or neither; the store always states a unit price on
    one of them. Derive the missing measurable axis from `price / unit_price`
    instead of treating the two axes as a contradiction.
#}
derivation_flagged as (
    select
        *,
        coalesce(
            unit_price_std_value > 0
                and price_value > 0
                and (
                    case_total_quantity is null
                    or (case_total_unit = 'ud' and unit_price_std_unit in ('kg', 'l'))
                ),
            false
        ) as is_unit_price_derived
    from typed
),
with_unit_price_fallback as (
    select
        * replace (
            case
                when is_unit_price_derived then safe_divide(price_value, unit_price_std_value)
                else case_total_quantity
            end as case_total_quantity,
            case
                when is_unit_price_derived then unit_price_std_unit
                else case_total_unit
            end as case_total_unit,
            case
                when is_unit_price_derived then unit_family
                else case_unit_family
            end as case_unit_family,
            case
                when is_unit_price_derived and case_type = 'unknown' then 'derived'
                else case_type
            end as case_type
        ),
        coalesce(
            explicit_count,
            case
                when unit_price_std_unit = 'ud' and unit_price_std_value > 0 and price_value > 0
                    then safe_divide(price_value, unit_price_std_value)
            end
        ) as case_count_quantity
    from derivation_flagged
),
with_raw_and_status as (
    select
        *,
        case
            when case_type = 'multipack' then regexp_extract(
                case_text_norm,
                r'((?:pack\s+(?:de\s+)?\d+.*?\b(?:de|x)\s*)?\d+\s*x?\s*\d*(?:\.\d+)?\s*(?:{{ meas }})\b)'
            )
            when case_unit_quantity is not null then regexp_extract(case_text_norm, r'(\d+(?:\.\d+)?\s*(?:{{ meas }})\b)')
            when explicit_count is not null then regexp_extract(
                case_text_norm,
                r'(\d+(?:\.\d+)?\s*(?:{{ count_words }})\b)'
            )
            when is_variable_weight then regexp_extract(case_text_norm, r'((?:al peso|peso variable))')
        end as case_size_raw,
        case
            when case_total_quantity > 0 then 'parsed'
            when is_variable_weight then 'ambiguous'
            else 'missing'
        end as case_parse_status,
        case
            when is_unit_price_derived then 'unit_price_ratio'
            when case_total_quantity > 0 then 'product_name_regex'
            when is_variable_weight then 'product_name_indicator'
        end as case_parse_method
    from with_unit_price_fallback
),
validated as (
    select
        *,
        case
            when case_parse_method = 'unit_price_ratio' then 'derived_from_unit_price'
            when case_total_quantity is null or unit_price_std_value is null then 'unavailable'
            when case_total_unit != unit_price_std_unit then
                {# The measurable axis disagrees, but a per-unit price can still be
                   corroborated against the count axis before calling it broken. #}
                case
                    when unit_price_std_unit = 'ud'
                     and case_count_quantity > 0
                     and abs(safe_divide(price_value, case_count_quantity) - unit_price_std_value)
                            / nullif(unit_price_std_value, 0) <= 0.03 then 'matched_unit_price'
                    else 'incompatible_units'
                end
            when abs(safe_divide(price_value, case_total_quantity) - unit_price_std_value)
                    / nullif(unit_price_std_value, 0) <= 0.03 then 'matched_unit_price'
            else 'unit_price_mismatch'
        end as case_validation_status
    from with_raw_and_status
)

select *
from validated
{{ incremental_scrape_date_filter() }}
