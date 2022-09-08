{{ config(materialized='view') }}

WITH source -- the CTE view name
	AS(
     SELECT {{ surrogate_key_int(['MARKET_ID']) }} AS DIM_MARKET_SK
          , *
       FROM {{ ref('VW_XD_DIV_CORP') }}
      WHERE 1 = 1
        AND PRCS_SYS = 'ASYS'
        AND INACT_DT IS NULL
        AND CONV_TO_DIV_NBR IS NULL
        AND DIV_TYP_CD IN('USF')
   ORDER BY MARKET_SK
    )

/* Outcome */
     SELECT *
       FROM SOURCE