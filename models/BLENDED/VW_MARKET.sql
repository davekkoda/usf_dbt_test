{{ config(materialized='view') }}

WITH source -- the CTE view name
	AS(
        SELECT MARKET_SK
          , MARKET_ID
          , MARKET_NM
          , MARKET_PRSDNT_NM
          , MARKET_TM_ZN
          , AREA_CD 
          , AREA_NM
          , AREA_ID
          , AREA_PRSDNT_NM
          , ZONE_CD
          , ZONE_ID
          , ZONE_NM
          , ZONE_PRSDNT_NM
          , RGN_CD
          , RGN_ID
          , RGN_NM
          , RGN_PRSDNT_NM
          , DIV_NM_CD_ID
          , GRP_NM_OR_DIV_NM_CD_ID
          , ADDR_LN_1
          , ADDR_LN_2
          , CITY
          , STATE
          , ZIP_CODE
          , PHONE_NB
          , DIV_SIZE_CD
          , DIV_TYPE_CD
          , DIV_TYPE_DESC
          , GROUP_TYPE_CD
          , GRP_CD
          , SFTY_RGN_CD
          , SFTY_RGN_ID
          , SFTY_RGN_NM
          , RSC_CD
          , RSC_NM
          , BUYR_TYPE_ID
          , ACQSTN_CMPNY_NM
          , ACQSTN_DT
          , TNDM_NODE
        FROM {{ ref('VW_XD_DIV_CORP') }}
        WHERE 1 = 1
            AND PRCS_SYS = 'ASYS'
            AND INACT_DT IS NULL
            AND CONV_TO_DIV_NBR IS NULL
            AND DIV_TYP_CD IN('USF')
    )

/* Outcome */
     SELECT *
       FROM SOURCE