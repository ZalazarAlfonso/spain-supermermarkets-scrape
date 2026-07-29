with base as (
    select *
    from {{ ref('silver_product_current') }}

)
select
    source_product_id,
    supermarket,
    product_url,
    product_name,
    group_std,
    category_std,
    subcategory_std,
    brand_raw,
    brand_norm,
    brand_std,
    brand_group,
    is_private_label,
    brand_parse_status,
    brand_parse_method,
    price_value,
    unit_base_unit_std,
    unit_family,
    unit_price_std_value,
    unit_price_std_unit,
    case_size_raw,
    case_pack_count,
    case_unit_quantity,
    case_unit_measure,
    case_count_measure,
    case_count_quantity,
    case_total_quantity,
    case_total_unit,
    case_unit_family,
    case_type,
    case_parse_status,
    case_parse_method,
    case_validation_status,
    offer_flag
FROM base
