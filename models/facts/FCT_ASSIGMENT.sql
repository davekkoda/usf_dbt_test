{{ config(materialized='incremental') }}

WITH source -- the CTE view name
	AS(
        SELECT SM.WCT_ID
        ,SM.WH_CD
        ,SM.USR_ID
        ,SM.DIRECT
        ,SM.REPORT_DATE
        ,SM.ISMEASURED
        ,SM.ACTUAL_SECONDS
        ,SM.GOAL_SECONDS
        ,SM.KVI_TMU_05
        ,SM.ASSIGN_NUM
        ,SM.JOBCODE_ID
        ,SM.KVISUMMARY_ID
        ,SM.SRC_ID
        ,SM.START_TIME
        ,SM.STOP_TIME
        ,ADJ.ADJ_DURATION 
<<<<<<< HEAD
        ,{{ dbt_utils.surrogate_key(['SM.WH_CD', 'SM.WCT_ID', 'SM.SRC_ID']) }} AS DIM_WORK_CATEGORY_SK
        ,{{ dbt_utils.surrogate_key(['SM.WH_CD']) }} AS DIM_MARKET_SK
        ,{{ dbt_utils.surrogate_key(['SM.WH_CD', 'SM.JOBCODE_ID', 'SM.SRC_ID']) }} AS DIM_JOBCODE_SK
        ,{{ dbt_utils.surrogate_key(['SM.WH_CD', 'SM.REPORT_DATE']) }} AS DIM_DRIVER_SK
=======
        ,ADJ.ADJ_INT_ID
        ,SM.LAST_UPD_DT
        ,{{ dbt_utils.surrogate_key(['SM.WH_ID', 'SM.WCT_ID', 'SM.SRC_ID']) }} AS DIM_WORK_CATEGORY_SK
        ,{{ dbt_utils.surrogate_key(['SM.WH_ID']) }} AS DIM_MARKET_SK
        ,{{ dbt_utils.surrogate_key(['SM.WH_ID', 'SM.JOBCODE_ID', 'SM.SRC_ID']) }} AS DIM_JOBCODE_SK
        ,{{ dbt_utils.surrogate_key(['SM.WH_ID', 'SM.REPORT_DATE']) }} AS DIM_DRIVER_SK
>>>>>>> a8fa696 (first version of icremental table for fact model)
        ,TO_NUMBER(TO_CHAR(TO_DATE(SM.REPORT_DATE),'YYYYMMDD'))AS DIM_DATE_SK
        FROM {{ ref( 'INT_KVI_SUMMARY') }} AS SM
        LEFT OUTER JOIN {{ ref('INT_KVI_ADJUSTMENTS') }} AS ADJ
        ON   SM.JOBCODE_ID = ADJ.JOBCODE_ID
        AND   SM.WH_CD = ADJ.WH_CD
        AND   SM.WCT_ID = ADJ.WCT_ID
        AND   SM.KVISUMMARY_ID = ADJ.KVISUMMARY_ID
        AND   SM.SRC_ID = ADJ.SRC_ID

    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
