{{ config(materialized='view') }}

WITH source -- the CTE view name
	AS(
        SELECT
            BRNCH_CD::integer AS  WH_ID -- A character code to represent Branch.`
            ,RGN_CD::varchar(16) AS RGN_CD  -- region id
            ,DIV_TYP_CD::varchar(16) AS DIV_TYP_CD  -- Division Type Code
            ,ZIP_CD::integer AS ZIP_CD  -- Division zip code
            ,DIV_NBR::integer AS DIV_ID  -- A 4-digit number given to each Division 
            ,CONV_TO_DIV_NBR::integer AS CONV_TO_DIV_NBR  -- Converted to Division Number
            ,PRCS_SYS::varchar(8) AS asd  -- Processing System
            ,DIV_NM::varchar(64) AS DIV_NM  -- Division Name
            ,DIV_NM_CD_NBR::varchar(128) AS DIV_NM_CD_NBR  --Div Name, Code and Number
            ,RGN_NM::varchar(64) AS RGN_NM --Name or Description of a Region Code
            ,AREA_NM::varchar(64) AS AREA_NM  -- Area Name
            ,SC_RGN_NM::varchar(64) AS SC_RGN_NM  -- Supply Chain Region Name
            ,CMPNY_DESC::varchar(64) AS CMPNY_DESC  -- Company Description
            ,INACT_DT::date AS INACT_DT  -- Inactive market
            ,CASE WHEN INACT_DT IS NULL THEN 1 ELSE 0 END AS IS_ACT
        FROM {{ source('GOLD_XDMADM', 'DIV_CORP') }}
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
