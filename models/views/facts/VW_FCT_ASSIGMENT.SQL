

{{ config(
    enabled=false,
    materialized='view'
)}}

WITH SOURCE AS(
SELECT 
        FCT.WCT_ID
        ,FCT.WH_CD
        ,FCT.USR_ID
        ,FCT.DIRECT
        ,FCT.REPORT_DATE
        ,FCT.ISMEASURED
        ,COALESCE(FCT.ACTUAL_SECONDS,0)
        ,COALESCE(FCT.GOAL_SECONDS,0)
        ,COALESCE(FCT.CASES,0)
        ,FCT.ASSIGN_NUM
        ,FCT.JOBCODE_ID
        ,FCT.KVISUMMARY_ID
        ,FCT.SRC_ID
        ,FCT.START_TIME
        ,FCT.STOP_TIME
        ,COALESCE(FCT.ADJ_DURATION,0)
        ,FCT.DIM_WORK_CATEGORY_SK
        ,FCT.DIM_MARKET_SK
        ,FCT.DIM_JOBCODE_SK
        ,FCT.DIM_DRIVER_SK
        ,FCT.DIM_DATE_SK
FROM
    {{ ref ('FCT_ASSIGMENT')}} as FCT
WHERE
    1=1
)

SELECT * FROM SOURCE