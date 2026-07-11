select *
from {{ ref('standard_rules_v1') }}
where
    cast(rule_id as string) = ''
    or trim(level) = ''
    or trim(target) = ''
    or trim(field) = ''
    or trim(match_type) = ''
    or trim(pattern) = ''
    or cast(priority as string) = ''
