{{ config(materialized="view") }}

WITH source 
    AS (
     SELECT WAR_INT_ID AS WORK_AREA_ID
          , WAR_NAME AS WORK_AREA_NM
          , WH_ID AS MARKET_ID
          , SRC_ID
       FROM {{source('GOLD_RED_PRAIRIE', 'WORKAREA')}} 
    )

/* Outcome */
     SELECT *
       FROM SOURCE