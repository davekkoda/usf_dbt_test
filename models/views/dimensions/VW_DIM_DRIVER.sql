{{ config(
    enabled=false,
    materialized='view'
)}}

WITH
     SOURCE AS (SELECT DIM_DRIVER_SK
                     , WH_CD
                     , RPRT_DT_TM::DATE
                     , DRVR_ID
                     , DRVR_NM
                  FROM {{ref ("DIM_DRIVER")}}
                 WHERE 1=1
               )
/* Outcome */
     SELECT *
       FROM SOURCE