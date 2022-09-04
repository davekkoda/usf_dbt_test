{{ config(materialized='incremental') }}

{% if target.name == 'dev' %}
{%  set env_user = "SNOW_USFBM_USERNAME_DEV" %}
{% elif target.name == 'qa' %}
{% set env_user = "SNOW_USFBM_USERNAME_QA" %}
{% elif target.name == 'prod' %}
{% set env_user = "SNOW_USFBM_USERNAME_PROD" %}
{% endif %}

WITH source 
     AS (
     SELECT 
          {{ surrogate_key_int(['EMPLOYEE_ID']) }} AS DIM_EMPLOYEE_SK
          , TO_NUMBER(TO_CHAR(TO_DATE(wfc.DATE_ID),'YYYYMMDD'))AS DIM_DATE_SK
          {{ surrogate_key_int(['PAY_CODE_ID']) }} AS DIM_PAY_CODE_SK 
          {{ surrogate_key_int(['LABOR_ACCT_ID']) }} AS DIM_LABOR_ACCT_SK
          , wfc.START_TMST
          , wfc.END_TMST
          , wfc.DURATION_SECS_QTY
          , wfc.TIME_ZONE_ID
          , wfc.WAGE_AMT
          , wfc.MONEY_AMT
          , CURRENT_DATE() AS LAST_UPDATE_DT
          , '{{ env_var(env_user) }}' AS MODIFIED_USER_ID
          , '{{ env_var(env_user) }}' AS LAST_MODIFIED_USER_ID
       FROM {{ ref('VW_WFC_TOTAL') }} wfc
     )
                     
/* Outcome */
     SELECT *
       FROM SOURCE

