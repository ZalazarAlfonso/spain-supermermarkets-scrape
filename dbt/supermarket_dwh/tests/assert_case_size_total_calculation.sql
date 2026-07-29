with expected as (
    select
        product_snapshot_id,
        product_name,
        case_total_quantity,
        case
            when case_unit_measure = 'kg' then case_pack_count * case_unit_quantity
            when case_unit_measure = 'g' then case_pack_count * case_unit_quantity / 1000
            when case_unit_measure = 'l' then case_pack_count * case_unit_quantity
            when case_unit_measure = 'cl' then case_pack_count * case_unit_quantity / 100
            when case_unit_measure = 'ml' then case_pack_count * case_unit_quantity / 1000
        end as expected_total_quantity
    from {{ ref('int_product_case_size_parsed') }}
    where case_unit_measure is not null
)

select *
from expected
where abs(case_total_quantity - expected_total_quantity) > 0.000001
