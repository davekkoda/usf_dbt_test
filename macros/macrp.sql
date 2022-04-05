{% macro get_max_last_upd() %}
{% if execute and is_incremental()%}
 {% set query %}
    SELECT max(LAST_UPD_DT) FROM {{ this }};
 {% endset %}
  {% set max_last_upd = run_query(query).columns[0][0] %}
  {% do return(LAST_UPD_DT) %}
{% endif %}
{% endmacro %}