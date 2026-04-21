with base as (

    select *
    from {{ ref('silver_product_current') }}

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
-- Join products and tule to get fields, and obtain the field value to evaluate.
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
        any_value(target) as target, -- we already checked that target is the same for all rules with the same rule_id
        any_value(priority) as priority, -- we already checked that priority is the same for all rules with the same rule_id
        count(*) as condition_count,
        countif(condition_matched) as matched_conditions, -- see how we count over the column with count_if in a boolean column
    from condition_results
    group by
        product_snapshot_id,
        level,
        rule_id
    having
        countif(condition_matched) = count(*) -- only keep rules where all conditions are matched

),
-- Now we rank the matches by priority so we get the higher ones first.
ranked_matches as (
    select
        *,
        row_number() over (
            partition by product_snapshot_id
            order by priority desc, rule_id asc
        ) as match_rank
    from matched_rules_grouped

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
-- Final join with the normalized products to get the category and subcategory standarized, replacing the ones without a match with "Other"
final as (
    select
        p.*,
        coalesce(m.category_std, "Other") as category_std,
        coalesce(m.subcategory_std, "Other") as subcategory_std
    from normalized_products p
    left join best_matches m
        on p.product_snapshot_id = m.product_snapshot_id

)

select *
from final