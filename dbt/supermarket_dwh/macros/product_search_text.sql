{#
    Accent-free, lower-case text to match a search or a shopping-list item
    against: the normalized product name plus the standardized brand, which the
    name often omits (Mercadona lists "Queso semicurado mezcla" under Hacendado).
#}
{% macro product_search_text() -%}
    trim(concat(product_norm, ' ', coalesce({{ normalize_text('brand_std') }}, '')))
{%- endmacro %}
