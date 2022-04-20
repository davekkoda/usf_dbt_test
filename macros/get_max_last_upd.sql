{% macro get_max_last_upd() %}
{% if execute and is_incremental()%}
 {% set query %}
    SELECT max(SRC_LAST_UPDATE_DT) FROM {{ this }};
 {% endset %}
  {% set max_last_upd = run_query(query).columns[0][0] %}
  {% do return(max_last_upd) %}
{% endif %}
{% endmacro %}