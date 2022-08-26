

WITH source -- the CTE view name
	AS(
        SELECT SM.WCT_INT_ID AS WCT_ID
        ,SM.WH_ID AS WH_CD
        ,SM.USR_ID
        ,SM.DIRECT
        ,SM.REPORT_DATE AS REPORT_DT
        ,SM.ISMEASURED
        ,SM.ACTUAL_SECONDS
        ,SM.GOAL_SECONDS
        ,SM.KVI_TMU_05 AS CASES_QTY
        ,SM.ASSIGN_NUM AS ASSIGN_NB
        ,SM.JOBCODEINTID AS JOBCODE_ID
        ,SM.KVISUMMARYINTID AS KVISUMMARY_ID
        ,SM.SRC_ID
        ,SM.START_TIME
        ,SM.STOP_TIME
        ,SM.LAST_UPD_DT::TIMESTAMP AS LAST_UPD_TMST_SRC
        ,SM.INS_USER_ID
        ,SM.LAST_UPD_USER_ID 
        FROM GOLD_DEV.RED_PRAIRIE.KVI_SUMMARY AS SM
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename