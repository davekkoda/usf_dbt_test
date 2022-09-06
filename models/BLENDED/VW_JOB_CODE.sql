{{ config(materialized='incremental', unique_key='JOB_CODE_SK') }}

{% if target.name == 'dev' %}
{%  set env_user = "SNOW_USFBM_USERNAME_DEV" %}
{% elif target.name == 'qa' %}
{% set env_user = "SNOW_USFBM_USERNAME_QA" %}
{% elif target.name == 'prod' %}
{% set env_user = "SNOW_USFBM_USERNAME_PROD" %}
{% endif %}

WITH area_group 
    AS(
     SELECT aa.AISLE_AREA_ID
          , aa.AISLE_AREA_NM
          , aa.AISLE_AREA_DSC
          , wa.WORK_AREA_NM
          , aa.MARKET_ID
          , aa.PICK_TYPE
          , aa.PICK_OFFSET
          , aa.SRC_ID
       FROM {{ ref('VW_RP_AISLE_AREA') }} aa
  LEFT JOIN {{ ref('VW_RP_WORK_AREA') }} wa
         ON wa.WORK_AREA_ID = aa.WORK_AREA_ID 
        AND wa.MARKET_ID = aa.MARKET_ID 
        AND wa.SRC_ID = aa.SRC_ID
    ),

WITH job_group 
    AS(
     SELECT j.JOB_CODE_ID
          , j.JOB_CODE_NM
          , j.MARKET_ID
          , j.CUSTOM_WORK_CATEGORY
          , wc.WORK_CATEGORY
          , j.JOB_CODE_DSC
          , j.AISLE_AREA_ID
          , j.AISLE_AREA_NM
          , j.DIRECT_FLG
          , j.BREAK_FLG
          , j.MEASURED_FLG
          , j.PAID_FLG
          , j.REQ_APPROVAL_FLG
          , j.MASK_LEVEL
          , j.EXT_ASSIGNMENT_TIME
          , j.INT_ASSIGNMENT_TIME
          , j.EXT_ORDER_TIME
          , j.INT_ORDER_TIME
          , j.SRC_ID
       FROM {{ ref('VW_RP_JOB_CODE') }} j
  LEFT JOIN {{ ref('VW_RP_WORK_CATEGORY') }} wc
         ON j.WORK_CATEGORY_ID = wc.WORK_CATEGORY_ID 
        AND j.MARKET_ID = wc.MARKET_ID
        AND j.SRC_ID = wc.SRC_ID
    ),

WITH source -- the CTE view name
	AS(
     SELECT {{ surrogate_key_int(['JOB_CODE_ID', 'SRC_ID']) }} AS DIM_JOB_CODE_SK
          , jg.JOB_CODE_ID
          , jg.JOB_CODE_NM
          , jg.JOB_CODE_DSC
          , jg.MARKET_ID
          , jg.CUSTOM_WORK_CATEGORY
          , jg.WORK_CATEGORY
          , ag.WORK_AREA_NM
          , jg.AISLE_AREA_NM
          , ag.AISLE_AREA_DSC
          , jg.DIRECT_FLG
          , jg.BREAK_FLG
          , jg.MEASURED_FLG
          , jg.PAID_FLG
          , jg.REQ_APPROVAL_FLG
          , ag.PICK_TYPE
          , ag.PICK_OFFSET
          , jg.MASK_LEVEL
          , jg.EXT_ASSIGNMENT_TIME
          , jg.INT_ASSIGNMENT_TIME
          , jg.EXT_ORDER_TIME
          , jg.INT_ORDER_TIME
          , jg.SRC_ID
          , CURRENT_DATE() AS LAST_UPDATE_DT
          , '{{ env_var(env_user) }}' AS MODIFIED_USER_ID
          , '{{ env_var(env_user) }}' AS LAST_MODIFIED_USER_ID
       FROM job_group jg
  LEFT JOIN area_group ag
         ON jg.AISLE_AREA_ID = ag.AISLE_AREA_ID
        AND jg.AISLE_AREA_NM = ag.AISLE_AREA_NM
        AND jg.MARKET_ID = ag.MARKET_ID
        AND jg.SRC_ID = ag.SRC_ID
   ORDER BY JOB_CODE_ID
    )

/* Outcome */
     SELECT *
       FROM SOURCE