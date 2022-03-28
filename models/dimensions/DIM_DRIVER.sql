WITH source -- the CTE view name
	AS(
        SELECT BRNCH_CD
            ,RPRT_DT_TM AS RPRT_DT_DM
            ,DRVR_ID 
        FROM {{ source('GOLD_OMNITRACS', 'DRIVER_LOG_HDR') }}
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
