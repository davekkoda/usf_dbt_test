{{ config(materialized="view") }}

WITH source 
    AS (
    SELECT AISLEAREA_INT_ID AS AISLE_AREA_ID
          , AISLE_AREA_ID AS AISLE_AREA_NM
          , DESCRIPTION AS AISLE_AREA_DSC
          , WAR_INT_ID AS WORK_AREA_ID
          , WH_ID AS MARKET_ID
          , PICK_TYPE
          , PICK_OFFSET
          , SRC_ID
       FROM {{source('GOLD_RED_PRAIRIE', 'AISLE_AREA')}} 
    )

/* Outcome */
     SELECT *
       FROM SOURCE