WITH source -- the CTE view name
	AS(
        SELECT 
            BRNCH_CD::varchar(8) AS BRNCH_CD
            ,RPRT_DT_TM::timestamp AS RPRT_DT_DM
            ,DRVR_ID::integer AS DRVR_ID
        FROM {{ source('GOLD_OMNITRACS', 'DRIVER_LOG_HDR') }}
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
