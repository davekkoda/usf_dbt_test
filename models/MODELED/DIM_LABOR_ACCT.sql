{{ config(materialized='incremental', unique_key='LABOR_ACCT_SK') }}

{% if target.name == 'dev' %}
{%  set env_user = "SNOW_USFBM_USERNAME_DEV" %}
{% elif target.name == 'qa' %}
{% set env_user = "SNOW_USFBM_USERNAME_QA" %}
{% elif target.name == 'prod' %}
{% set env_user = "SNOW_USFBM_USERNAME_PROD" %}
{% endif %}

WITH source -- the CTE view name
	AS(
        SELECT DISTINCT
            {{ surrogate_key_int(['LABOR_ACCT_ID']) }} AS LABOR_ACCT_SK
            , *
            , CURRENT_DATE() AS LAST_UPDATE_DT
            ,'{{ env_var(env_user) }}' AS MODIFIED_USER_ID
            ,'{{ env_var(env_user) }}' AS LAST_MODIFIED_USER_ID
        FROM {{ ref('VW_LABOR_ACCT') }}
        ORDER BY 
            DIM_WORK_CATEGORY_SK
    )

SELECT * FROM source -- from the CTE view build a new reference with this filename