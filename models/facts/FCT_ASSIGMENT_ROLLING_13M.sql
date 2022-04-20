{% if target.name == 'dev' %}
{%  set env_user = "SNOW_USFBM_USERNAME_DEV" %}
{% elif target.name == 'qa' %}
{% set env_user = "SNOW_USFBM_USERNAME_QA" %}
{% elif target.name == 'prod' %}
{% set env_user = "SNOW_USFBM_USERNAME_PROD" %}
{% endif %}

WITH source -- the CTE view name
    AS(
        SELECT 
        FCT.WCT_ID
        ,FCT.WH_CD
        ,FCT.USR_ID
        ,FCT.DIRECT
        ,FCT.REPORT_DATE
        ,FCT.ISMEASURED
        ,COALESCE(FCT.ACTUAL_SECONDS,0) AS ACTUAL_SECONDS
        ,COALESCE(FCT.GOAL_SECONDS,0) AS GOAL_SECONDS
        ,COALESCE(FCT.CASES,0) AS CASES
        ,FCT.ASSIGN_NUM
        ,FCT.JOBCODE_ID
        ,FCT.KVISUMMARY_ID
        ,FCT.SRC_ID
        ,FCT.START_TIME
        ,FCT.STOP_TIME
        ,FCT.SRC_LAST_UPDATE_DT
        ,COALESCE(FCT.ADJ_DURATION,0) AS ADJ_DURATION
        , CURRENT_DATE() AS LAST_UPDATE_DT
        ,'{{ env_var(env_user) }}' AS MODIFIED_USER_ID
        ,'{{ env_var(env_user) }}' AS LAST_MODIFIED_USER_ID
        ,FCT.DIM_WORK_CATEGORY_SK
        ,FCT.DIM_MARKET_SK
        ,FCT.DIM_JOBCODE_SK
        ,FCT.DIM_DRIVER_SK
        ,FCT.DIM_DATE_SK
        FROM {{ ref( 'FCT_ASSIGMENT') }} AS FCT
        WHERE
        1=1
        AND  FCT.REPORT_DATE BETWEEN DATE_TRUNC(Month,ADD_MONTHS(CURRENT_DATE,-13)) AND DATEADD(day,-1,current_date)
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename

