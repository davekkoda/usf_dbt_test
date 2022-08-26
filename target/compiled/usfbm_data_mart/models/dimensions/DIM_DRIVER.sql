





WITH source -- the CTE view name
	AS(
        SELECT 
            MD5_NUMBER_UPPER64(cast(coalesce(cast(WH_CD as 
    varchar
), '') || '-' || coalesce(cast(RPRT_TMST::DATE as 
    varchar
), '') as 
    varchar
)) AS DIM_DRIVER_SK -- 'DRVR_ID', 'DRVR_NM'
            , *
            , CURRENT_DATE() AS LAST_UPDATE_DT
            ,'W846026' AS MODIFIED_USER_ID
            ,'W846026' AS LAST_MODIFIED_USER_ID
        FROM CDW_DEV.SANDBOX_W846026.VW_DRIVER_LOG_HDR
        ORDER BY DIM_DRIVER_SK
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename