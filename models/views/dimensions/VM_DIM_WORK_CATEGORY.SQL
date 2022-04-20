{{ config(
    enabled=false,
    materialized='view'
)}}

WITH
     SOURCE AS (     SELECT DIM_WORK_CATEGORY_PK
          , WCT_ID
          , WCT_NAME
          , WH_CD
       FROM {{ref ("DIM_WORK_CATEGORY")}}
      WHERE 1=1)
/* Outcome */
     SELECT *
       FROM SOURCE