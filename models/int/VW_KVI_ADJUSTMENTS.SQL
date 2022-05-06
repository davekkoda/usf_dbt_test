{{ config(materialized='view') }}
WITH source -- the CTE view name
	AS(
        SELECT ADJ.ADJ_DURATION 
        ,ADJ.ADJ_INT_ID
        ,ADJ.JOBCODEINTID AS JOBCODE_ID
        ,ADJ.WH_ID AS WH_CD
        ,ADJ.WCT_INT_ID AS WCT_ID
        ,ADJ.KVISUMMARYINTID AS KVISUMMARY_ID
        ,ADJ.SRC_ID
        ,ADJ.LAST_UPD_DT::TIMESTAMP AS LAST_UPD_TMST_SRC
        ,ADJ.INS_USER_ID
        ,ADJ.LAST_UPD_USER_ID
        ,ADJ.REPORT_DATE AS REPORT_DT
        FROM {{ source('GOLD_RED_PRAIRIE', 'KVI_ADJUSTMENTS') }} AS ADJ

    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
