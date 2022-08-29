{{ config{materialized="view"} }}

WITH source 
    AS (
    SELECT aa.AISLEAREA_INT_ID AS AISLE_AREA_ID
          , aa.AISLE_AREA_ID AS AISLE_AREA_NM
          , aa.DESCRIPTION AS AISLE_AREA_DSC
          , wa.WAR_NAME AS WORK_AREA_NM
          , aa.WH_ID AS MARKET_ID
          , aa.PICK_TYPE
          , aa.PICK_OFFSET
          , aa.SRC_ID
       FROM {{source('GOLD_RED_PRAIRIE', 'AISLE_AREA')}} aa
  LEFT JOIN {{source('GOLD_RED_PRAIRIE', 'WORKAREA')}} wa
         ON wa.war_int_id = aa.war_int_id
       AND wa.wh_id = aa.wh_id
       AND wa.src_id = aa.src_id
    )

SELECT * FROM source;