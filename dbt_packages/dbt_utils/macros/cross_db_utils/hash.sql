{% macro hash(field) -%}
  {{ return(adapter.dispatch('hash', 'dbt_utils') (field)) }}
{%- endmacro %}

-- EDITED TO USE MD5_NUMBER_UPPER64 and not MD5

{% macro default__hash(field) -%}
    MD5_NUMBER_UPPER64(cast({{field}} as {{dbt_utils.type_string()}}))
{%- endmacro %}


{% macro bigquery__hash(field) -%}
    to_hex({{dbt_utils.default__hash(field)}})
{%- endmacro %}
