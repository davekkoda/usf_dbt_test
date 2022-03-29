WITH source -- the CTE view name
	AS(
<<<<<<< HEAD
        SELECT
            {{ dbt_utils.surrogate_key(['BRNCH_CD']) }} AS DIM_MARKET_SK
            ,BRNCH_CD -- A character code to represent Branch.`
=======
        SELECT BRNCH_CD -- A character code to represent Branch.`
>>>>>>> main
            ,RGN_CD -- region id
            ,DIV_TYP_CD -- Division Type Code
            ,ZIP_CD -- Division zip code
            ,DIV_NBR -- A 4-digit number given to each Division 
            ,CONV_TO_DIV_NBR -- Converted to Division Number
            ,PRCS_SYS -- Processing System
            ,DIV_NM -- Division Name
            ,DIV_NM_CD_NBR --Div Name, Code and Number
            ,RGN_NM --Name or Description of a Region Code
            ,AREA_NM -- Area Name
            ,SC_RGN_NM -- Supply Chain Region Name
            ,CMPNY_DESC -- Company Description
            ,INACT_DT -- Inactive market
            ,CASE
        WHEN INACT_DT IS NULL THEN 1
        ELSE 0 END AS IS_ACT
        FROM {{ source('GOLD_XDMADM', 'DIV_CORP') }}
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
