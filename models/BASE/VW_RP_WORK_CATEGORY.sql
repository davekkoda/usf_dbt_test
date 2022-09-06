{{ config(materialized="view") }}

WITH source 
    AS (
     SELECT WCT_INT_ID AS WORK_CATEGORY_ID
          , WCT_NAME AS WORK_CATEGORY_NM
          , WH_ID AS MARKET_ID
          , SRC_ID
       FROM {{source('GOLD_RED_PRAIRIE', 'WORKCATEGORY')}} 
    )

/* Outcome */
     SELECT *
       FROM SOURCE