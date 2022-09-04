{{ config(materialized='view') }}

WITH source -- the CTE view name
	AS(
     SELECT KVISUMMARYINTID AS KVI_SUMMARY_ID
          , USR_ID AS EMPLOYEE_ID
          , PLAN_DATE::DATE AS DATE_ID
          , WH_ID AS MARKET_ID
          , JOBCODEINTID AS JOB_CODE_ID
          , WCT_INT_ID AS WORK_CATEGORY_ID
          , ADJ_START_TIME AS ADJ_START_TS
          , ADJ_DURATION
          , ADJ_BLENDED_COST
          , ROUTE_NUM
          , ASSIGN_NUM AS ASSIGN_NB
          , APPROVED_BY
          , SRC_ID
       FROM {{ source('GOLD_RED_PRAIRIE', 'KVI_ADJUSTMENTS') }} 
    )

/* Outcome */
     SELECT *
       FROM SOURCE