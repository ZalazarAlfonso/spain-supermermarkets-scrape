{#
    Removes screen-reader boilerplate that Alcampo's navigation leaks into the
    scraped category/subcategory labels (e.g. "mejillones (se abre en una
    ventana nueva)"). Applied to the normalized value only, so `*_raw` keeps the
    original text for auditing. Without this any `equals` taxonomy rule silently
    misses the affected shelves.
#}
{% macro strip_nav_noise(expression) -%}
    nullif(
        trim(
            regexp_replace(
                regexp_replace(
                    {{ expression }},
                    r'\(\s*se abre en una ventana nueva\s*\)',
                    ' '
                ),
                r'\s+',
                ' '
            )
        ),
        ''
    )
{%- endmacro %}
