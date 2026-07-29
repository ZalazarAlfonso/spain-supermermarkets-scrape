with raw_counts as (
    select
        regexp_replace({{ normalize_text('brand_raw') }}, r'[^a-z0-9]+', ' ') as brand_match_norm,
        trim(brand_raw) as brand_raw,
        count(*) as occurrence_count
    from {{ ref('silver_product_snapshots') }}
    where brand_raw is not null
      and trim(brand_raw) != ''
    group by 1, 2
),
valid_raw as (
    select *
    from raw_counts
    where length(brand_match_norm) >= 3
      and regexp_contains(brand_match_norm, r'[a-z]')
      and brand_match_norm not in (
          'producto', 'la', 'el', 'central', 'dr', 'casa', 'don', 'san', 'santa',
          'gran', 'font', 'ben', 'bo', 'marca', 'sin marca', 'varios', 'para',
          'queso', 'sabor', 'seleccion', 'tierra', 'iberico', 'gourmet', 'calidad',
          'pan', 'pro', 'patatas', 'croquetas', 'limon', 'garcia'
      )
),
raw_candidates as (
    select
        brand_match_norm,
        initcap(lower(array_agg(brand_raw order by occurrence_count desc, length(brand_raw) desc limit 1)[offset(0)])) as brand_std,
        cast(null as string) as brand_group,
        cast(null as string) as supermarket,
        false as is_private_label,
        50 as priority,
        'observed_brand_raw' as dictionary_source
    from valid_raw
    group by brand_match_norm
),
seed_candidates as (
    select
        regexp_replace({{ normalize_text('brand_alias_norm') }}, r'[^a-z0-9]+', ' ') as brand_match_norm,
        brand_std,
        nullif(trim(brand_group), '') as brand_group,
        nullif(trim(supermarket), '') as supermarket,
        cast(is_private_label as bool) as is_private_label,
        cast(priority as int64) as priority,
        'brand_alias_seed' as dictionary_source
    from {{ ref('brand_aliases_v1') }}
),
combined as (
    select * from raw_candidates
    union all
    select * from seed_candidates
)

select *
from combined
qualify row_number() over (
    partition by brand_match_norm, coalesce(supermarket, '')
    order by priority desc, dictionary_source desc
) = 1
