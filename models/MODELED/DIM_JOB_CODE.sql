{{ config(materialized='incremental', unique_key='JOB_CODE_SK') }}

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
            {{ surrogate_key_int(['JOB_CODE_ID', 'JOB_CODE_NM', 'MARKET_ID', 'SRC_ID']) }} AS DIM_JOB_CODE_SK
          , JOB_CODE_ID
          , JOB_CODE_NM
          , JOB_CODE_DSC
          , MARKET_ID
          , CUSTOM_WORK_CATEGORY
          , WORK_CATEGORY
          , WORK_AREA_NM
          , AISLE_AREA_NM
          , AISLE_AREA_DSC
          , DIRECT_FLG
          , BREAK_FLG
          , MEASURED_FLG
          , PAID_FLG
          , REQ_APPROVAL_FLG
          , PICK_TYPE
          , PICK_OFFSET
          , MASK_LEVEL
          , EXT_ASSIGNMENT_TIME
          , INT_ASSIGNMENT_TIME
          , EXT_ORDER_TIME
          , INT_ORDER_TIME
          , SRC_ID
          , CURRENT_DATE() AS LAST_UPDATE_DT
          , '{{ env_var(env_user) }}' AS MODIFIED_USER_ID
          , '{{ env_var(env_user) }}' AS LAST_MODIFIED_USER_ID
       FROM {{ ref('VW_JOB_CODE') }}
    )

/* Outcome */
     SELECT *
       FROM SOURCE