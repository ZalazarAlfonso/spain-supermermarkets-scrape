{% macro normalize_text(column_name) -%}
    nullif(
        regexp_replace(
            trim(
                regexp_replace(
                    regexp_replace(
                        normalize_and_casefold(cast({{ column_name }} as string), NFKD),
                        r'\pM',
                        ''
                    ),
                    r'\s+',
                    ' '
                )
            ),
            r'\s+',
            ' '
        ),
        ''
    )
{%- endmacro %}
