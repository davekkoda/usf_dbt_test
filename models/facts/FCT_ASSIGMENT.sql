{{ config(materialized='incremental') }}

WITH source -- the CTE view name
    AS(
        SELECT SM.WCT_ID
        ,SM.WH_CD
        ,SM.USR_ID
        ,SM.DIRECT
        ,SM.REPORT_DATE
        ,SM.ISMEASURED
        ,COALESCE(SM.ACTUAL_SECONDS,0) AS ACTUAL_SECONDS
        ,COALESCE(SM.GOAL_SECONDS,0) AS GOAL_SECONDS
        ,COALESCE(SM.CASES,0) AS CASES
        ,SM.ASSIGN_NUM
        ,SM.JOBCODE_ID
        ,SM.KVISUMMARY_ID
        ,SM.SRC_ID
        ,SM.START_TIME
        ,SM.STOP_TIME
        ,COALESCE(SM.LAST_UPD_DT, '2017-01-05 04:40:22.566 -0800'::TIMESTAMP) AS LAST_UPD_DT
        ,COALESCE(ADJ.ADJ_DURATION,0) AS ADJ_DURATION
        ,{{ dbt_utils.surrogate_key(['SM.WH_CD', 'SM.WCT_ID', 'SM.SRC_ID']) }} AS DIM_WORK_CATEGORY_SK
        ,{{ dbt_utils.surrogate_key(['SM.WH_CD']) }} AS DIM_MARKET_SK
        ,{{ dbt_utils.surrogate_key(['SM.WH_CD', 'SM.JOBCODE_ID', 'SM.SRC_ID']) }} AS DIM_JOBCODE_SK
        ,{{ dbt_utils.surrogate_key(['SM.WH_CD', 'SM.REPORT_DATE']) }} AS DIM_DRIVER_SK
        ,TO_NUMBER(TO_CHAR(TO_DATE(SM.REPORT_DATE),'YYYYMMDD'))AS DIM_DATE_SK
        FROM {{ ref( 'INT_KVI_SUMMARY') }} AS SM
        LEFT OUTER JOIN {{ ref('INT_KVI_ADJUSTMENTS') }} AS ADJ
        ON   SM.JOBCODE_ID = ADJ.JOBCODE_ID
        AND   SM.WH_CD = ADJ.WH_CD
        AND   SM.WCT_ID = ADJ.WCT_ID
        AND   SM.KVISUMMARY_ID = ADJ.KVISUMMARY_ID
        AND   SM.SRC_ID = ADJ.SRC_ID

        {% if is_incremental() %}

<<<<<<< HEAD
<<<<<<< HEAD
        where LAST_UPD_DT > (select max(LAST_UPD_DT) from {{ this }})

        {% endif %}


=======
        -- this filter will only be applied on an incremental run
=======
>>>>>>> 3b4e32b (Working Incremental FCT table in CICD qa branch)
        where LAST_UPD_DT > '{{ get_max_last_upd() }}'

        {% endif %}

<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 45eeb6c (Incremental CICD testing)
=======
=======
>>>>>>> 5818666 (Great Expectations testing)
>>>>>>> 6533919 (Great Expectations testing)
=======
>>>>>>> 1d41593 (rebase and merge with qa-cicd)
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
