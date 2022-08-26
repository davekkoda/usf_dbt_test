





WITH source -- the CTE view name
	AS(
        SELECT
            TO_NUMBER(TO_CHAR(TO_DATE(CLNDR_DT),'YYYYMMDD'))AS DIM_DATE_SK
            , *
            , CURRENT_DATE() AS LAST_UPDATE_DT
            ,'W846026' AS MODIFIED_USER_ID
            ,'W846026' AS LAST_MODIFIED_USER_ID
        FROM CDW_DEV.SANDBOX_W846026.VW_TIME_CORP
        ORDER BY DIM_DATE_SK
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename