{{ config(materialized='view') }}
WITH source -- the CTE view name
	AS(
        SELECT ADJ.ADJ_DURATION 
        ,ADJ.ADJ_INT_ID
        ,ADJ.JOBCODEINTID AS JOBCODE_ID
        ,ADJ.WH_ID
        ,ADJ.WCT_INT_ID AS WCT_ID
        ,ADJ.KVISUMMARYINTID
        ,ADJ.SRC_ID
        FROM {{ source('GOLD_RED_PRAIRIE', 'KVI_ADJUSTMENTS') }} AS ADJ

    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
