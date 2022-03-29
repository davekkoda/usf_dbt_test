WITH source -- the CTE view name
	AS(
        SELECT SM.WCT_ID
        ,SM.WH_ID
        ,SM.USR_ID
        ,SM.DIRECT
        ,SM.REPORT_DATE
        ,SM.ISMEASURED
        ,SM.ACTUAL_SECONDS
        ,SM.GOAL_SECONDS
        ,SM.KVI_TMU_05
        ,SM.ASSIGN_NUM
        ,SM.JOBCODE_ID
        ,SM.KVISUMMARYINTID
        ,SM.SRC_ID
        ,ADJ.ADJ_DURATION 
        ,ADJ.ADJ_INT_ID
        ,{{ dbt_utils.surrogate_key(['SM.WH_ID', 'SM.WCT_INT_ID', 'SM.SRC_ID']) }} AS DIM_WORK_CATEGORY_SK
        ,{{ dbt_utils.surrogate_key(['SM.WH_ID']) }} AS DIM_MARKET_SK
        ,{{ dbt_utils.surrogate_key(['SM.WH_ID', 'SM.JOBCODEINTID', 'SM.SRC_ID']) }} AS DIM_JOBCODE_SK
        ,{{ dbt_utils.surrogate_key(['SM.WH_ID', 'SM.REPORT_DATE']) }} AS DIM_DRIVER_SK
        ,TO_NUMBER(TO_CHAR(TO_DATE(SM.REPORT_DATE),'YYYYMMDD'))AS DATE_SK
        FROM {{ ref( 'INT_KVI_SUMMARY') }} AS SM
        LEFT OUTER JOIN {{ ref('INT_KVI_ADJUSTMENTS') }} AS ADJ
        ON   SM.JOBCODE_ID = ADJ.JOBCODE_ID
        AND   SM.WH_ID = ADJ.WH_ID
        AND   SM.WCT_INT_ID = ADJ.WCT_INT_ID
        AND   SM.KVISUMMARYINTID = ADJ.KVISUMMARYINTID
        AND   SM.SRC_ID = ADJ.SRC_ID
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
