-- One row per source_product_id with the canonical ID of its chain of URL
-- changes (the earliest ID). Products never relinked map to themselves.
with recursive chains as (
    select
        p.source_product_id,
        p.source_product_id as canonical_product_id,
        0 as link_depth
    from (
        select distinct source_product_id
        from {{ ref('silver_product_standardized') }}
    ) p
    where p.source_product_id not in (
        select next_product_id from {{ ref('silver_product_identity_links') }}
    )

    union all

    select
        l.next_product_id as source_product_id,
        c.canonical_product_id,
        c.link_depth + 1 as link_depth
    from chains c
    join {{ ref('silver_product_identity_links') }} l
        on l.previous_product_id = c.source_product_id
)

select *
from chains
