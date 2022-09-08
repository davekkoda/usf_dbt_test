{{ config(materialized='incremental') }}

{% if target.name == 'dev' %}
{%  set env_user = "SNOW_USFBM_USERNAME_DEV" %}
{% elif target.name == 'qa' %}
{% set env_user = "SNOW_USFBM_USERNAME_QA" %}
{% elif target.name == 'prod' %}
{% set env_user = "SNOW_USFBM_USERNAME_PROD" %}
{% endif %}

WITH wfctotal 
     AS(
     SELECT WFC_TOTAL_ID
          , TIMESHEET_ID
          , DATE_ID
          , EMPLOYEE_ID
          , PAY_CODE_ID
          , LABOR_ACCT_ID
          , START_TMST
          , END_TMST
          , DURATION_SECS_QTY
          , TIME_ZONE_ID
          , WAGE_AMT
          , MONEY_AMT
       FROM {{ ref('VW_KR_WFC_TOTAL') }}
     ),

WITH employee
     AS(
     SELECT EMP_NB
          , PERSON_ID
       FROM {{ ref('VW_KR_PERSON') }}
     ),

WITH source 
     AS(
     SELECT {{ surrogate_key_int(['EMP_NB']) }} AS DIM_EMPLOYEE_SK
          , emp.EMP_NB
          , wfc.EMPLOYEE_ID
          , TO_NUMBER(TO_CHAR(TO_DATE(wfc.DATE_ID), 'YYYYMMDD')) AS DIM_DATE_SK
          , {{ surrogate_key_int(['PAY_CODE_ID']) }} AS DIM_PAY_CODE_SK
          , {{ surrogate_key_int(['LABOR_ACCT_ID']) }} AS DIM_LABOR_ACCT_SK
          , wfc.START_TMST
          , wfc.END_TMST
          , wfc.DURATION_SECS_QTY
          , wfc.TIME_ZONE_ID
          , wfc.WAGE_AMT
          , wfc.MONEY_AMT
       FROM wfctotal wfc
       JOIN employee emp
          ON emp.PERSON_ID = wfc.EMPLOYEE_ID
     )
                     
/* Outcome */
     SELECT *
       FROM SOURCE

