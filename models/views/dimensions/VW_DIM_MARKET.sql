{{ config(
    enabled=false,
    materialized='view'
)}}

WITH
     SOURCE AS (SELECT DIM_MARKET_SK
                     , CMPNY_DESC
                     , AREA_NM
                     , RGN_CD
                     , RGN_NM
                     , DIV_ID
                     , DIV_NM
                     , DIV_NM_CD_NBR
                     , DIV_TYP_CD
                     , ZIP_CD
                     , WH_CD
                     , PRCS_SYS
                  FROM {{ref ("DIM_MARKET")}}
                 WHERE 1=1
                   AND IS_ACT = 1)
/* Outcome */
     SELECT *
       FROM SOURCE