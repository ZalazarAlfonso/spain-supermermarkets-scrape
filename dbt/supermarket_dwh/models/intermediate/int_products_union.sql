select * from {{ ref('stg_alcampo_products')}}
union all
select * from {{ ref('stg_carrefour_products') }}
union all
select * from {{ ref('stg_dia_products') }}
union all
select * from {{ref('stg_mercadona_products')}}