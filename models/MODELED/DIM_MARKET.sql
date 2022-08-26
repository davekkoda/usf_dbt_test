{{ config(materialized='incremental', unique_key='DIM_MARKET_PK') }}

{% if target.name == 'dev' %}
{%  set env_user = "SNOW_USFBM_USERNAME_DEV" %}
{% elif target.name == 'qa' %}
{% set env_user = "SNOW_USFBM_USERNAME_QA" %}
{% elif target.name == 'prod' %}
{% set env_user = "SNOW_USFBM_USERNAME_PROD" %}
{% endif %}

WITH source -- the CTE view name
	AS(
        SELECT 
            {{ surrogate_key_int(['MARKET_ID']) }} AS DIM_MARKET_SK 
            , *
            , CURRENT_DATE() AS LAST_UPDATE_DT
            ,'{{ env_var(env_user) }}' AS MODIFIED_USER_ID
            ,'{{ env_var(env_user) }}' AS LAST_MODIFIED_USER_ID
        FROM {{ ref('VW_DIV_CORP') }}
        ORDER BY DIM_MARKET_SK
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
