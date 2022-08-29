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
          , mkt.MARKET_SK
          , mkt.MARKET_ID
          , mkt.MARKET_NM
          , mkt.MARKET_PRSDNT_NM
          , mkt.MARKET_TM_ZN
          , mkt.AREA_CD
          , mkt.AREA_NM
          , mkt.AREA_ID
          , mkt.AREA_PRSDNT_NM
          , mkt.ZONE_CD
          , mkt.ZONE_ID
          , mkt.ZONE_NM
          , mkt.ZONE_PRSDNT_NM
          , mkt.RGN_CD
          , mkt.RGN_ID
          , mkt.RGN_NM
          , mkt.RGN_PRSDNT_NM
          , mkt.DIV_NM_CD_ID
          , mkt.GRP_NM_OR_DIV_NM_CD_ID
          , mkt.ADDR_LN_1
          , mkt.ADDR_LN_2
          , mkt.CITY
          , mkt.STATE
          , mkt.ZIP_CODE
          , mkt.PHN_NBR
          , mkt.DIV_SIZE_CD
          , mkt.DIV_TYPE_CD
          , mkt.DIV_TYPE_DESC
          , mkt.GROUP_TYPE_CD
          , mkt.GRP_CD
          , mkt.SFTY_RGN_CD
          , mkt.SFTY_RGN_ID
          , mkt.SFTY_RGN_NM
          , mkt.RSC_CD
          , mkt.RSC_NM
          , mkt.BUYR_TYPE_ID
          , mkt.ACQSTN_CMPNY_NM
          , mkt.ACQSTN_DT
          , mkt.TNDM_NODE
          , CURRENT_DATE() AS LAST_UPDATE_DT
          , '{{ env_var(env_user) }}' AS MODIFIED_USER_ID
          , '{{ env_var(env_user) }}' AS LAST_MODIFIED_USER_ID
       FROM {{ ref('VW_DIV_CORP') }} mkt
   ORDER BY MARKET_NM
    )

/* Outcome */
     SELECT *
       FROM SOURCE