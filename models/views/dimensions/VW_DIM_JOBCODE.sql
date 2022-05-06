{{ config(
    enabled=false,
    materialized='view'
)}}

WITH
     SOURCE AS (SELECT DIM_JOBCODE_SK
                     , DIRECT
                     , JOBCODE_CD
                     , WH_CD
                  FROM {{ref ("DIM_JOBCODE")}}
                 WHERE 1=1
               )
/* Outcome */
     SELECT *
       FROM SOURCE