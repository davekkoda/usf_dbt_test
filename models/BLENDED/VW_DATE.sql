{{ config(materialized='incremental', unique_key='DIM_DATE_SK') }}

{% if target.name == 'dev' %}
{%  set env_user = "SNOW_USFBM_USERNAME_DEV" %}
{% elif target.name == 'qa' %}
{% set env_user = "SNOW_USFBM_USERNAME_QA" %}
{% elif target.name == 'prod' %}
{% set env_user = "SNOW_USFBM_USERNAME_PROD" %}
{% endif %}

WITH source -- the CTE view name
	AS(
     SELECT TO_NUMBER(TO_CHAR(TO_DATE(CLNDR_DT), 'YYYYMMDD')) AS DIM_DATE_SK
          , CLNDR_DT
          , CLNDR_DAY_NM
          , CLNDR_MTH_NM
          , CLNDR_YR::INTEGER AS CLNDR_YR
          , CLNDR_DAY_OF_WK::INTEGER AS CLNDR_DAY_OF_WK
          , CLNDR_DAY_OF_MTH
          , CLNDR_WK_OF_MTH
          , CLNDR_DAY_OF_YR::INTEGER AS CLNDR_DAY_OF_YR
          , CLNDR_WK_OF_YR
          , CLNDR_MTH_OF_YR
          , CLNDR_QTR_OF_YR
          , CLNDR_HALF_OF_YR
          , CLNDR_YR_QTR
          , CLNDR_YR_MTH::INTEGER AS CLNDR_YR_MTH
          , FISC_YR
          , FISC_DAY_OF_YR::INTEGER AS FISC_DAY_OF_YR
          , FISC_DAY_NM
          , FISC_WK_OF_YR::INTEGER AS FISC_WK_OF_YR
          , FISC_WK_OF_PRD::INTEGER AS FISC_WK_OF_PRD
          , FISC_MTH_OF_YR
          , FISC_MTH_NM
          , FISC_QTR_OF_YR
          , FISC_HALF_OF_YR
          , FISC_YR_QTR
          , FISC_YR_MTH
          , FISC_YR_WK
          , FISC_WKS_THIS_PD
          , TM_SK_YRAGO
          , CLNDR_DT_YRAGO
          , FISC_PRD_NM
          , GRG_CLNDR_DT_YRAGO
          , CLNDR_WK_STRT_DT
          , CLNDR_WK_END_DT
          , CLNDR_WK_NM
          , CLNDR_YR_WK
          , CLNDR_MTH_NM_YR
          , GRG_CLNDR_YR_MTH_YRAGO
          , LIC_WED_DT
          , LIC_THURS_DT
          , FST_BUS_DT_NEXT_MONTH
          , ORD_GD_GEN_DT
          , MON_AFTER_LIC_THURS_DT
          , CLNDR_DT_WKAGO
          , CURRENT_DATE() AS LAST_UPDATE_DT
          , '{{ env_var(env_user) }}' AS MODIFIED_USER_ID
          , '{{ env_var(env_user) }}' AS LAST_MODIFIED_USER_ID
       FROM {{ ref('VW_TIME_CORP') }}
   ORDER BY DIM_DATE_SK
    )


/* Outcome */
     SELECT *
       FROM SOURCE