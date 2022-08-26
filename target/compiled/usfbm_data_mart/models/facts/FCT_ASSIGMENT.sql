





WITH source -- the CTE view name
    AS(
        SELECT SM.WCT_ID
        ,SM.WH_CD
        ,SM.USR_ID
        ,SM.DIRECT
        ,SM.REPORT_DT
        ,SM.ISMEASURED
        ,COALESCE(SM.ACTUAL_SECONDS,0) AS ACTUAL_SECONDS
        ,COALESCE(SM.GOAL_SECONDS,0) AS GOAL_SECONDS
        ,COALESCE(SM.CASES_QTY,0) AS CASES_QTY
        ,SM.ASSIGN_NB
        ,SM.JOBCODE_ID
        ,SM.KVISUMMARY_ID
        ,SM.SRC_ID
        ,SM.START_TIME
        ,SM.STOP_TIME
        ,COALESCE(SM.LAST_UPD_TMST_SRC,ADJ.LAST_UPD_TMST_SRC, '2017-01-05 04:40:22.566 -0800'::TIMESTAMP) AS SRC_LAST_UPDATE_TMST
        ,COALESCE(ADJ.ADJ_DURATION,0) AS ADJ_DURATION
        , CURRENT_DATE AS LAST_UPDATE_DT
        ,'W846026' AS MODIFIED_USER_ID
        ,'W846026' AS LAST_MODIFIED_USER_ID
        ,MD5_NUMBER_UPPER64(cast(coalesce(cast(SM.WH_CD as 
    varchar
), '') || '-' || coalesce(cast(SM.WCT_ID as 
    varchar
), '') || '-' || coalesce(cast(SM.SRC_ID as 
    varchar
), '') as 
    varchar
)) AS DIM_WORK_CATEGORY_SK
        ,MD5_NUMBER_UPPER64(cast(coalesce(cast(SM.WH_CD as 
    varchar
), '') as 
    varchar
)) AS DIM_MARKET_SK
        ,MD5_NUMBER_UPPER64(cast(coalesce(cast(SM.WH_CD as 
    varchar
), '') || '-' || coalesce(cast(SM.JOBCODE_ID as 
    varchar
), '') || '-' || coalesce(cast(SM.SRC_ID as 
    varchar
), '') as 
    varchar
)) AS DIM_JOBCODE_SK
        ,MD5_NUMBER_UPPER64(cast(coalesce(cast(SM.WH_CD as 
    varchar
), '') || '-' || coalesce(cast(SM.REPORT_DT as 
    varchar
), '') as 
    varchar
)) AS DIM_DRIVER_SK
        ,TO_NUMBER(TO_CHAR(TO_DATE(SM.REPORT_DT),'YYYYMMDD')) AS DIM_DATE_SK
        FROM CDW_DEV.SANDBOX_W846026.VW_KVI_SUMMARY AS SM
        LEFT OUTER JOIN CDW_DEV.SANDBOX_W846026.VW_KVI_ADJUSTMENTS AS ADJ
        ON   SM.JOBCODE_ID = ADJ.JOBCODE_ID
        AND   SM.WH_CD = ADJ.WH_CD
        AND   SM.WCT_ID = ADJ.WCT_ID
        AND   SM.KVISUMMARY_ID = ADJ.KVISUMMARY_ID
        AND   SM.SRC_ID = ADJ.SRC_ID

        

        where SRC_LAST_UPDATE_DT > '2021-09-02 18:34:51'

        
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename