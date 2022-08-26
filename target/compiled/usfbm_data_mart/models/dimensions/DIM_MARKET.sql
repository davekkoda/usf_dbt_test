





WITH source -- the CTE view name
	AS(
        SELECT 
            MD5_NUMBER_UPPER64(cast(coalesce(cast(WH_CD as 
    varchar
), '') as 
    varchar
)) AS DIM_MARKET_SK -- , 'DIV_ID'
            , *
            , CURRENT_DATE() AS LAST_UPDATE_DT
            ,'W846026' AS MODIFIED_USER_ID
            ,'W846026' AS LAST_MODIFIED_USER_ID
        FROM CDW_DEV.SANDBOX_W846026.VW_DIV_CORP
        ORDER BY DIM_MARKET_SK
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename