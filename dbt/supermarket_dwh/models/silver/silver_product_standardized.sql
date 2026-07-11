with base as (

    select *
    from {{ ref('silver_product_snapshots') }}

),
-- Normalize the product, category and subcategory columns using the macro
normalized_products as (

    select
        *,
        {{ normalize_text('category_raw') }} as category_norm,
        {{ normalize_text('subcategory_raw') }} as subcategory_norm,
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
    select distinct
        group_std,
        category_std,
        subcategory_std
    from standard_taxonomy
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

        case
            when pair.category_std is not null then pair.category_std
            when category_only.category_std is not null then category_only.category_std
            when subcategory_only.category_std is not null then subcategory_only.category_std
            else 'Other'
        end as category_std,

        case
            when pair.subcategory_std is not null then pair.subcategory_std
            when subcategory_only.subcategory_std is not null then subcategory_only.subcategory_std
            else 'Other'
        end as subcategory_std,

        coalesce(
            pair.group_std,
            category_only.group_std,
            subcategory_only.group_std,
            'Other'
        ) as group_std
    from best_matches m
    left join standard_taxonomy pair
        on m.category_std = pair.category_std
        and m.subcategory_std = pair.subcategory_std
    left join taxonomy_categories category_only
        on m.category_std = category_only.category_std
        and m.subcategory_std is null
    left join taxonomy_subcategories subcategory_only
        on m.subcategory_std = subcategory_only.subcategory_std
        and m.category_std is null
),
-- Final join with the normalized products to get standardized taxonomy values.
final as (
    select
        p.*,
        coalesce(m.group_std, 'Other') as group_std,
        coalesce(m.category_std, 'Other') as category_std,
        coalesce(m.subcategory_std, 'Other') as subcategory_std
    from normalized_products p
    left join resolved_matches m
        on p.product_snapshot_id = m.product_snapshot_id

)

select *
from final
