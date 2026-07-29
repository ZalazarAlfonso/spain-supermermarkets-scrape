{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={"field": "scrape_date", "data_type": "date"},
    cluster_by=["supermarket"],
    on_schema_change='sync_all_columns'
) }}

with base as (
    select
        *,
        regexp_replace({{ normalize_text('brand_raw') }}, r'[^a-z0-9]+', ' ') as brand_raw_norm,
        regexp_replace({{ normalize_text('product_name') }}, r'[^a-z0-9]+', ' ') as product_brand_match_text
    from {{ ref('silver_product_snapshots') }}
),
prepared as (
    select
        *,
        brand_raw_norm is not null
          and length(brand_raw_norm) >= 3
          and regexp_contains(brand_raw_norm, r'[a-z]')
          and brand_raw_norm not in (
              'producto', 'la', 'el', 'central', 'dr', 'casa', 'don', 'san', 'santa',
              'gran', 'font', 'ben', 'bo', 'marca', 'sin marca', 'varios', 'para',
              'queso', 'sabor', 'seleccion', 'tierra', 'iberico', 'gourmet', 'calidad',
              'pan', 'pro', 'patatas', 'croquetas', 'limon', 'garcia'
          ) as has_valid_brand_raw
    from base
),
product_override_rows as (
    select
        source_product_id,
        supermarket,
        nullif(trim(brand_std), '') as brand_std,
        cast(is_no_brand as bool) as is_no_brand,
        override_source
    from {{ ref('brand_product_overrides_v1') }}
    union all
    select
        source_product_id,
        supermarket,
        case
            when lower(trim(brand_inferred)) = 'sin marca' then null
            else nullif(trim(brand_inferred), '')
        end as brand_std,
        lower(trim(brand_inferred)) = 'sin marca' as is_no_brand,
        brand_inference_method as override_source
    from {{ ref('brand_reviewed_products_v1') }}
    where brand_link_review_status != 'not_in_low_medium_scope'
),
product_overrides as (
    select
        *,
        regexp_replace({{ normalize_text('brand_std') }}, r'[^a-z0-9]+', ' ') as brand_match_norm
    from product_override_rows
),
override_metadata as (
    select
        o.source_product_id,
        d.brand_group,
        d.is_private_label
    from product_overrides o
    join {{ ref('int_brand_dictionary') }} d
      on o.brand_match_norm = d.brand_match_norm
     and (d.supermarket is null or d.supermarket = o.supermarket)
    where not o.is_no_brand
    qualify row_number() over (
        partition by o.source_product_id
        order by d.priority desc, d.supermarket is not null desc
    ) = 1
),
direct_alias_candidates as (
    select
        p.product_snapshot_id,
        d.brand_match_norm,
        d.brand_std,
        d.brand_group,
        d.is_private_label,
        d.dictionary_source,
        d.priority
    from prepared p
    join {{ ref('int_brand_dictionary') }} d
      on p.brand_raw_norm = d.brand_match_norm
     and (d.supermarket is null or d.supermarket = p.supermarket)
    where p.has_valid_brand_raw
    qualify row_number() over (
        partition by p.product_snapshot_id
        order by d.priority desc, d.supermarket is not null desc
    ) = 1
),
inferred_candidates as (
    select
        p.product_snapshot_id,
        d.brand_match_norm,
        d.brand_std,
        d.brand_group,
        d.is_private_label,
        d.dictionary_source,
        d.priority
    from prepared p
    join {{ ref('int_brand_dictionary') }} d
      on strpos(
          concat(' ', p.product_brand_match_text, ' '),
          concat(' ', d.brand_match_norm, ' ')
      ) > 0
     and (d.supermarket is null or d.supermarket = p.supermarket)
    where not p.has_valid_brand_raw
      and length(d.brand_match_norm) >= 3
      and (regexp_contains(d.brand_match_norm, r'[a-z]') or d.dictionary_source = 'brand_alias_seed')
      and d.dictionary_source = 'brand_alias_seed'
    qualify row_number() over (
        partition by p.product_snapshot_id
        order by length(d.brand_match_norm) desc, d.priority desc, d.supermarket is not null desc
    ) = 1
),
final as (
    select
        p.* except (brand_raw_norm, product_brand_match_text, has_valid_brand_raw),
        case
            when o.is_no_brand then null
            else coalesce(o.brand_match_norm, a.brand_match_norm, i.brand_match_norm, p.brand_raw_norm)
        end as brand_norm,
        case
            when o.is_no_brand then null
            when o.brand_std is not null then o.brand_std
            when a.brand_std is not null then a.brand_std
            when p.has_valid_brand_raw then initcap(lower(trim(p.brand_raw)))
            else i.brand_std
        end as brand_std,
        case
            when o.is_no_brand then null
            else coalesce(om.brand_group, a.brand_group, i.brand_group)
        end as brand_group,
        coalesce(om.is_private_label, a.is_private_label, i.is_private_label, false) as is_private_label,
        case
            when o.is_no_brand then 'confirmed_no_brand'
            when o.brand_std is not null then 'product_override'
            when a.dictionary_source = 'brand_alias_seed' then 'mapped'
            when a.brand_std is not null or p.has_valid_brand_raw then 'normalized_only'
            when i.brand_std is not null then 'inferred_from_product_name'
            else 'missing'
        end as brand_parse_status,
        case
            when o.is_no_brand or o.brand_std is not null then 'product_override_seed'
            when a.dictionary_source = 'brand_alias_seed' then 'brand_alias_seed'
            when a.brand_std is not null or p.has_valid_brand_raw then 'brand_raw'
            when i.brand_std is not null then 'product_name_dictionary'
        end as brand_parse_method
    from prepared p
    left join product_overrides o
      on p.source_product_id = o.source_product_id
     and p.supermarket = o.supermarket
    left join override_metadata om
      on p.source_product_id = om.source_product_id
    left join direct_alias_candidates a
      on p.product_snapshot_id = a.product_snapshot_id
    left join inferred_candidates i
      on p.product_snapshot_id = i.product_snapshot_id
)

select *
from final
{{ incremental_scrape_date_filter() }}
