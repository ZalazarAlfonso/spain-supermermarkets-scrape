{#
    Shared incremental predicate for the append-only scrape_date lineage
    (int_products_union -> ... -> silver_product_standardized -> fact_product_price_snapshots).

    Uses a lookback window instead of a strict `> max(scrape_date)` cutoff so that a
    late-arriving or re-run scrape for a past date self-heals on the next build
    instead of being silently skipped. insert_overwrite replaces whole partitions,
    so re-processing a few extra days is safe and idempotent.
#}
{% macro incremental_scrape_date_filter(lookback_days=3) -%}
{% if is_incremental() %}
where scrape_date >= (
    select date_sub(max(scrape_date), interval {{ lookback_days }} day)
    from {{ this }}
)
{% endif %}
{%- endmacro %}
