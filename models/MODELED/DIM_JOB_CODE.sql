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
            , jc.JOB_CODE_ID
            , jc.JOB_CODE_NM
            , jc.JOB_CODE_DSC
            , jc.MARKET_ID
            , jc.CUSTOM_WORK_CATEGORY
            , jc.WORK_CATEGORY
            , wa.WORK_AREA_NM
            , jc.AISLE_AREA_NM
            , wa.AISLE_AREA_DSC
            , jc.DIRECT_FLG
            , jc.BREAK_FLG
            , jc.MEASURED_FLG
            , jc.PAID_FLG
            , jc.REQ_APPROVAL_FLG
            , wa.PICK_TYPE
            , wa.PICK_OFFSET
            , jc.MASK_LEVEL
            , jc.EXT_ASSIGNMENT_TIME
            , jc.INT_ASSIGNMENT_TIME
            , jc.EXT_ORDER_TIME
            , jc.INT_ORDER_TIME
            , jc.SRC_ID
            , CURRENT_DATE() AS LAST_UPDATE_DT
            ,'{{ env_var(env_user) }}' AS MODIFIED_USER_ID
            ,'{{ env_var(env_user) }}' AS LAST_MODIFIED_USER_ID
        FROM {{ ref('VW_JOB_CODE') }} jc
        LEFT JOIN {{ ref('VW_WORK_AREA') }} wa
        ON jc.AISLE_AREA_ID = wa.AISLE_AREA_ID 
            AND jc.AISLE_AREA_NM = wa.AISLE_AREA_NM 
            AND jc.MARKET_ID = wa.MARKET_ID 
            AND jc.SRC_ID = wa.SRC_ID 
    )

/* Outcome */
     SELECT *
       FROM SOURCE