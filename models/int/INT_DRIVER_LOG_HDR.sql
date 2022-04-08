{{ config(materialized='view') }}

WITH source -- the CTE view name
	AS(
        SELECT 
            BRNCH_CD::varchar(8) AS WH_CD
            ,RPRT_DT_TM::timestamp AS RPRT_DT_TM
            ,DRVR_ID::varchar(16) AS DRVR_ID
            ,DRVR_NM::varchar(64) AS DRVR_NM
        FROM {{ source('GOLD_OMNITRACS', 'DRIVER_LOG_HDR') }}
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename