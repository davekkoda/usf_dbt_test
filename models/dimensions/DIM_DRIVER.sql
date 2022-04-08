{{ config(materialized='incremental', unique_key='DIM_DRIVER_PK') }}
{% if target.name == 'dev' %}
{%  set env_user = "SNOW_USFBM_USERNAME_DEV" %}
{% elif target.name == 'qa' %}
{% set env_user = "SNOW_USFBM_USERNAME_QA" %}
{% elif target.name == 'prod' %}
{% set env_user = "SNOW_USFBM_USERNAME_QA" %}
{% endif %}
WITH source -- the CTE view name
	AS(
        SELECT 
            {{ dbt_utils.surrogate_key(['WH_CD', 'RPRT_DT_TM', 'DRVR_ID', 'DRVR_NM']) }} AS DIM_DRIVER_PK
            , *
            , CURRENT_DATE() AS LAST_UPDATE_DT
            ,'{{ env_var(env_user) }}' AS MODIFIED_USER_ID
            ,'{{ env_var(env_user) }}' AS LAST_MODIFIED_USER_ID
        FROM {{ ref('INT_DRIVER_LOG_HDR') }}
        ORDER BY DIM_DRIVER_PK
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
