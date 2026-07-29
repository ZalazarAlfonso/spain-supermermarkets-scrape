select
    product_snapshot_id,
    supermarket,
    product_name,
    brand_std
from {{ ref('int_product_brand_standardized') }}
where brand_parse_status not in ('product_override', 'confirmed_no_brand')
  and lower(trim(brand_std)) in (
    'producto', 'la', 'el', 'central', 'dr', 'casa', 'don', 'san', 'santa',
    'gran', 'font', 'ben', 'bo', 'marca', 'sin marca', 'varios', 'para',
    'queso', 'sabor', 'seleccion', 'tierra', 'iberico', 'gourmet', 'calidad',
    'pan', 'pro', 'patatas', 'croquetas', 'limon', 'garcia'
)
