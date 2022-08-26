{{ config(materialized='view') }}

WITH source -- the CTE view name
	AS(
        SELECT DIV_SK AS MARKET_SK
          , DIV_NBR AS MARKET_ID
          , DIV_NM AS MARKET_NM
          , DIV_PRSDNT_NM AS MARKET_PRSDNT_NM
          , DIV_TM_ZN AS MARKET_TM_ZN
          , AREA_CD 
          , AREA_NM
          , AREA_NBR AS AREA_ID
          , AREA_PRSDNT_NM
          , ZONE_CD
          , ZONE_NBR AS ZONE_ID
          , ZONE_NM
          , ZONE_PRSDNT_NM
          , RGN_CD
          , RGN_NBR AS RGN_ID
          , RGN_NM
          , RGN_PRSDNT_NM
          , DIV_NM_CD_NBR AS DIV_NM_CD_ID
          , GRP_NM_OR_DIV_NM_CD_NBR AS GRP_NM_OR_DIV_NM_CD_ID
          , ADDR_LN_1
          , ADDR_LN_2
          , CITY
          , ST AS STATE
          , ZIP_CD AS ZIP_CODE
          , PHN_NBR
          , DIV_SZ_CD AS DIV_SIZE_CD
          , DIV_TYP_CD AS DIV_TYPE_CD
          , DIV_TYP_DESC AS DIV_TYPE_DESC
          , GRP_TYP_CD AS GROUP_TYPE_CD
          , GRP_CD
          , SFTY_RGN_CD
          , SFTY_RGN_NBR AS SFTY_RGN_ID
          , SFTY_RGN_NM
          , RSC_CD
          , RSC_NM
          , BUYR_TYP_ID AS BUYR_TYPE_ID
          , ACQSTN_CMPNY_NM
          , ACQSTN_DT
          , TNDM_NODE
        FROM {{ source('GOLD_XDMADM', 'DIV_CORP') }}
        WHERE 1 = 1
            AND PRCS_SYS = 'ASYS'
            AND INACT_DT IS NULL
            AND CONV_TO_DIV_NBR IS NULL
            AND DIV_TYP_CD IN('USF')
    )

SELECT * FROM source -- from the CTE view build a new reference with this filename
