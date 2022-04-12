{{ config(materialized='incremental') }}

{% if target.name == 'dev' %}
{%  set env_user = "SNOW_USFBM_USERNAME_DEV" %}
{% elif target.name == 'qa' %}
{% set env_user = "SNOW_USFBM_USERNAME_QA" %}
{% elif target.name == 'prod' %}
{% set env_user = "SNOW_USFBM_USERNAME_PROD" %}
{% endif %}

WITH source -- the CTE view name
    AS(
        SELECT SM.WCT_ID
        ,SM.WH_CD
        ,SM.USR_ID
        ,SM.DIRECT
        ,SM.REPORT_DATE
        ,SM.ISMEASURED
        ,COALESCE(SM.ACTUAL_SECONDS,0) AS ACTUAL_SECONDS
        ,SM.GOAL_SECONDSAS GOAL_SECONDS
        ,COALESCE(SM.CASES,0) AS CASES
        ,SM.ASSIGN_NUM
        ,SM.JOBCODE_ID
        ,SM.KVISUMMARY_ID
        ,SM.SRC_ID
        ,SM.START_TIME
        ,SM.STOP_TIME
        ,COALESCE(SM.LAST_UPD_DT_SRC,ADJ.LAST_UPD_DT_SRC, '2017-01-05 04:40:22.566 -0800'::TIMESTAMP) AS SRC_LAST_UPDATE_DT
        ,COALESCE(ADJ.ADJ_DURATION,0) AS ADJ_DURATION
        , CURRENT_DATE() AS LAST_UPDATE_DT
        ,'{{ env_var(env_user) }}' AS MODIFIED_USER_ID
        ,'{{ env_var(env_user) }}' AS LAST_MODIFIED_USER_ID
        ,{{ dbt_utils.surrogate_key(['SM.WH_CD', 'SM.WCT_ID', 'SM.SRC_ID']) }} AS DIM_WORK_CATEGORY_SK
        ,{{ dbt_utils.surrogate_key(['SM.WH_CD']) }} AS DIM_MARKET_SK
        ,{{ dbt_utils.surrogate_key(['SM.WH_CD', 'SM.JOBCODE_ID', 'SM.SRC_ID']) }} AS DIM_JOBCODE_SK
        ,{{ dbt_utils.surrogate_key(['SM.WH_CD', 'SM.REPORT_DATE']) }} AS DIM_DRIVER_SK
        ,TO_NUMBER(TO_CHAR(TO_DATE(SM.REPORT_DATE),'YYYYMMDD')) AS DIM_DATE_SK
        FROM {{ ref( 'INT_KVI_SUMMARY') }} AS SM
        LEFT OUTER JOIN {{ ref('INT_KVI_ADJUSTMENTS') }} AS ADJ
        ON   SM.JOBCODE_ID = ADJ.JOBCODE_ID
        AND   SM.WH_CD = ADJ.WH_CD
        AND   SM.WCT_ID = ADJ.WCT_ID
        AND   SM.KVISUMMARY_ID = ADJ.KVISUMMARY_ID
        AND   SM.SRC_ID = ADJ.SRC_ID

        {% if is_incremental() %}

        where SRC_LAST_UPDATE_DT > '{{ get_max_last_upd() }}'

        {% endif %}
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename

