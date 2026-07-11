-- Test consistency on rules to confirm the rules do what they are intended to do checking the values on the columns are logical.
select
    level,
    rule_id
from {{ ref('standard_rules_v1') }}
group by
    level,
    rule_id
having
    count(distinct target) > 1
    or count(distinct priority) > 1
