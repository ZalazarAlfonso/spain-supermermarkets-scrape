{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={"field": "scrape_date", "data_type": "date"},
    cluster_by=["supermarket"],
    on_schema_change='sync_all_columns'
) }}

with unioned as (
    select * from {{ ref('stg_alcampo_products')}}
    union all
    select * from {{ ref('stg_carrefour_products') }}
    union all
    select * from {{ ref('stg_dia_products') }}
    union all
    select * from {{ref('stg_mercadona_products')}}
)

select *
from unioned
{{ incremental_scrape_date_filter() }}
