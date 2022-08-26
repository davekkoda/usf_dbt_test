





WITH source -- the CTE view name
	AS(
        SELECT DISTINCT
            MD5_NUMBER_UPPER64(cast(coalesce(cast(WH_CD as 
    varchar
), '') || '-' || coalesce(cast(WCT_ID as 
    varchar
), '') || '-' || coalesce(cast(SRC_ID as 
    varchar
), '') as 
    varchar
)) AS DIM_WORK_CATEGORY_SK
            , *
            , CURRENT_DATE() AS LAST_UPDATE_DT
            ,'W846026' AS MODIFIED_USER_ID
            ,'W846026' AS LAST_MODIFIED_USER_ID
        FROM CDW_DEV.SANDBOX_W846026.VW_WORKCATEGORY
        WHERE WCT_NM != '<none>'
        ORDER BY DIM_WORK_CATEGORY_SK
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename