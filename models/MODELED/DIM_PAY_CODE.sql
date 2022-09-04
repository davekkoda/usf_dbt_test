{{ config(materialized='incremental', unique_key='DIM_PAY_CODE_SK') }}

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
          {{ surrogate_key_int(['PAY_CODE_ID']) }} AS DIM_PAY_CODE_SK 
          , pc.PAY_CODE_ID
          , pc.PAY_CODE_NM
          , pc.PAY_CODE_CATEGORY
          , pc.PAY_CODE_TYPE
          , pc.EXCUSED_ABSENCE_FLG
          , pc.WAGE_ADDITION
          , pc.WAGE_MULTIPLY
          , pc.CDW_UPD_TS
          , pc.CDW_UPD_USR_ID
          , CURRENT_DATE() AS LAST_UPDATE_DT
          , '{{ env_var(env_user) }}' AS MODIFIED_USER_ID
          , '{{ env_var(env_user) }}' AS LAST_MODIFIED_USER_ID
       FROM {{ ref('VW_PAY_CODE') }} pc
   ORDER BY PAY_CODE_ID
    )

/* Outcome */
     SELECT *
       FROM SOURCE