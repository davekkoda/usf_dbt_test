{{ config(
    enabled=false,
    materialized='view'
)}}

WITH
     SOURCE AS (SELECT FCT.CLNDR_WK_STRT_DT
                     , FCT.DIM_MARKET_SK
                     , FCT.DIM_JOBCODE_SK
                     , FCT.DAYS_NIGHT
                     , FCT.MEASURED_DIRECT
                     , FCT.MEASURED_INDIRECT
                     , UNMEASURED_DIRECT
                     , UNMEASURED_INDIRECT
                     , UNPAID_BREAK
                     , INSERTED_DIRECT
                     , INSERTED_INDIRECT AS "Inserted Indirect Hours"
                     , GOAL_HOURS AS "Goal Hours"
                  FROM {{ref ('FCT_ASSIGMENT_WK_SMRY')}} AS FCT
                 WHERE 1=1
               )
/* Outcome */
     SELECT *
       FROM SOURCE