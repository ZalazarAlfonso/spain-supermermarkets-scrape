{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={"field": "scrape_date", "data_type": "date"},
    cluster_by=["supermarket"],
    on_schema_change='sync_all_columns'
) }}

with base as (

    select *
    from {{ ref('int_product_brand_standardized') }}

),
-- Normalize the product, category and subcategory columns using the macro
normalized_products as (

    select
        *,
        {{ strip_nav_noise(normalize_text('category_raw')) }} as category_norm,
        {{ strip_nav_noise(normalize_text('subcategory_raw')) }} as subcategory_norm,
        {{ normalize_text('product_name') }} as product_norm
    from base

),
-- Rules data normalization for use.
rules as (
    select
        cast(rule_id as int64) as rule_id,
        level,
        target,
        field,
        match_type,
        pattern,
        nullif(trim(supermarket), '') as supermarket,
        cast(priority as int64) as priority
    from 
        {{ref('standard_rules_v1')}}
      
),
-- Get standard taxonomy for evaluating rules
standard_taxonomy as (
    select
        cast(group_order as int64) as group_order,
        cast(category_order as int64) as category_order,
        cast(subcategory_order as int64) as subcategory_order,
        group_std,
        category_std,
        subcategory_std,
        category_norm,
        subcategory_norm
    from {{ ref('standard_taxonomy_v1') }}
),
taxonomy_categories as (
    select distinct
        group_std,
        category_std
    from standard_taxonomy
),
taxonomy_subcategories as (
    select
        subcategory_std,
        min(group_std) as group_std,
        min(category_std) as category_std
    from standard_taxonomy
    group by subcategory_std
),
-- Join products and rule to get fields, and obtain the field value to evaluate.
rule_conditions as (
    select
        p.product_snapshot_id,
        p.supermarket,
        p.category_norm,
        p.subcategory_norm,
        p.product_norm,

        r.level,
        r.rule_id,
        r.target,
        r.priority,
        r.field,
        r.match_type,
        r.pattern,

        case r.field
            when 'category_norm' then p.category_norm
            when 'subcategory_norm' then p.subcategory_norm
            when 'product_norm' then p.product_norm
        end as field_value

        from normalized_products p
        join rules r
            on r.supermarket is null
            or r.supermarket = p.supermarket

),
-- Evaluate each rule condition
condition_results as (
    select
        product_snapshot_id,
        supermarket,
        level,
        rule_id,
        target,
        priority,
        field,
        match_type,
        pattern,
        field_value,
        
        case
            when field_value is null then false
            when match_type = 'equals' then field_value = pattern
            when match_type in ('contains', 'regex') then regexp_contains(field_value, pattern)
            else false
        end as condition_matched

    from rule_conditions
),

matched_rules_grouped as (
    select
        product_snapshot_id,
        level,
        rule_id,
        min(target) as target, -- rule consistency is checked by a dbt test
        min(priority) as priority, -- rule consistency is checked by a dbt test
        count(*) as condition_count,
        sum(case when condition_matched then 1 else 0 end) as matched_conditions
    from condition_results
    group by
        product_snapshot_id,
        level,
        rule_id

),
matched_rules as (
    select *
    from matched_rules_grouped
    where matched_conditions = condition_count -- only keep rules where all conditions are matched
),
-- Now we rank the matches by priority so we get the higher ones first.
ranked_matches as (
    select
        *,
        row_number() over (
            partition by product_snapshot_id, level
            order by priority desc, rule_id asc
        ) as match_rank
    from matched_rules

),
-- We are keeping now only the first ranked matches for each column.
best_matches as (

    select
        product_snapshot_id,

        max(case when level = 'category' and match_rank = 1 then target end) as category_std,
        max(case when level = 'subcategory' and match_rank = 1 then target end) as subcategory_std
    
    from ranked_matches
    group by product_snapshot_id
),
resolved_matches as (
    select
        m.product_snapshot_id,

        -- A specific subcategory is the strongest signal. Resolve its canonical
        -- parent from the taxonomy instead of discarding both matches when a
        -- noisy source category points at a different parent.
        coalesce(subcategory_match.category_std, category_match.category_std, 'Other') as category_std,
        coalesce(subcategory_match.subcategory_std, 'Other') as subcategory_std,
        coalesce(subcategory_match.group_std, category_match.group_std, 'Other') as group_std
    from best_matches m
    left join taxonomy_subcategories subcategory_match
        on m.subcategory_std = subcategory_match.subcategory_std
    left join taxonomy_categories category_match
        on m.category_std = category_match.category_std
),
-- Join the normalized products to get the rule-based taxonomy values.
rule_mapped as (
    select
        p.*,
        coalesce(m.group_std, 'Other') as group_std,
        coalesce(m.category_std, 'Other') as category_std,
        coalesce(m.subcategory_std, 'Other') as subcategory_std
    from normalized_products p
    left join resolved_matches m
        on p.product_snapshot_id = m.product_snapshot_id

),
batch as (
    select *
    from rule_mapped
    {{ incremental_scrape_date_filter() }}
),
-- Every mapped observation of a product, from this batch and from partitions
-- already built. Promotional sections (Dia "descubrimientos") and broken source
-- navigation file a product under a node no rule can place, but the same
-- product URL is usually mapped on other days.
mapped_history as (
    select source_product_id, scrape_date, group_std, category_std, subcategory_std
    from batch
    where category_std != 'Other'
    {% if is_incremental() %}
    union all
    select source_product_id, scrape_date, group_std, category_std, subcategory_std
    from {{ this }}
    where category_std != 'Other'
      and scrape_date < (select min(scrape_date) from batch)
    {% endif %}
),
product_taxonomy_memory as (
    select
        source_product_id,
        group_std,
        category_std,
        subcategory_std
    from mapped_history
    qualify row_number() over (
        partition by source_product_id
        order by scrape_date desc, category_std, subcategory_std
    ) = 1
),
final as (
    select
        b.* except (group_std, category_std, subcategory_std),
        case when b.category_std = 'Other' then coalesce(t.group_std, b.group_std) else b.group_std end as group_std,
        case when b.category_std = 'Other' then coalesce(t.category_std, b.category_std) else b.category_std end as category_std,
        case when b.category_std = 'Other' then coalesce(t.subcategory_std, b.subcategory_std) else b.subcategory_std end as subcategory_std,
        case
            when b.category_std != 'Other' then 'rules'
            when t.category_std is not null then 'inherited_from_product'
            else 'unmapped'
        end as taxonomy_method
    from batch b
    left join product_taxonomy_memory t
        on b.source_product_id = t.source_product_id
)

select *
from final
