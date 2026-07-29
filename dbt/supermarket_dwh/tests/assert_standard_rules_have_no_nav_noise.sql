{#
    strip_nav_noise() removes Alcampo's screen-reader boilerplate from the
    normalized category/subcategory labels, so a rule pattern that still contains
    that text can never match anything. Two curated rules were silently orphaned
    this way; this keeps a new one from being written against the raw junk.
#}
select
    rule_id,
    level,
    target,
    field,
    pattern
from {{ ref('standard_rules_v1') }}
where field in ('category_norm', 'subcategory_norm')
  and regexp_contains(lower(pattern), r'se abre en una ventana nueva')
