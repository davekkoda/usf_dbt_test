WITH source -- the CTE view name
	AS(
        SELECT SM.WCT_INT_ID
        ,SM.WH_ID
        ,SM.USR_ID
        ,SM.DIRECT
        ,SM.REPORT_DATE
        ,SM.ISMEASURED
        ,SM.ACTUAL_SECONDS
        ,SM.GOAL_SECONDS
        ,SM.KVI_TMU_05
        ,SM.ASSIGN_NUM
        ,SM.JOBCODEINTID
        ,SM.KVISUMMARYINTID
        ,SM.SRC_ID
        ,ADJ.ADJ_DURATION 
        ,ADJ.ADJ_INT_ID
        ,{{ dbt_utils.surrogate_key(['SM.WH_ID', 'SM.WCT_INT_ID', 'SM.SRC_ID']) }} AS DIM_WORK_CATEGORY_SK
        ,{{ dbt_utils.surrogate_key(['SM.WH_ID']) }} AS DIM_MARKET_SK
        ,{{ dbt_utils.surrogate_key(['SM.WH_ID', 'SM.JOBCODEINTID', 'SM.SRC_ID']) }} AS DIM_JOBCODE_SK
        ,{{ dbt_utils.surrogate_key(['SM.BRNCH_CD', 'SM.RPRT_DT_DM']) }} AS DIM_DRIVER_SK
        FROM {{ source('GOLD_RED_PRAIRIE', 'KVI_SUMMARY') }} AS SM
        LEFT OUTER JOIN {{ source('GOLD_RED_PRAIRIE', 'KVI_ADJUSTMENTS') }} AS ADJ
        ON   SM.JOBCODEINTID = ADJ.JOBCODEINTID
        AND   SM.WH_ID = ADJ.WH_ID
        AND   SM.WCT_INT_ID = ADJ.WCT_INT_ID
        AND   SM.KVISUMMARYINTID = ADJ.KVISUMMARYINTID
        AND   SM.SRC_ID = ADJ.SRC_ID
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
