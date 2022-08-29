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
     SELECT
          , {{ surrogate_key_int(['EMPLOYEE_ID']) }} AS DIM_EMPLOYEE_SK
          , {{ surrogate_key_int(['MARKET_ID']) }} AS DIM_MARKET_SK
          , {{ surrogate_key_int(['JOB_CODE_ID', 'JOB_CODE_NM', 'MARKET_ID', 'SRC_ID']) }} AS DIM_JOB_CODE_SK
          , TO_NUMBER(TO_CHAR(TO_DATE(kvi.PLAN_DATE), 'YYYYMMDD')) AS DIM_DATE_SK
          , {{ surrogate_key_int(['SUPERVISOR_ID']) }} AS SUPERVISOR_SK
          , kvi.STARTLOCATIONID AS START_LOC_ID
          , kvi.ENDLOCATIONID AS END_LOC_ID
          , kvi.START_TIME AS START_TS
          , kvi_adj.ADJ_START_TS
          , kvi.STOP_TIME AS END_TS
          , kvi.ACTUAL_SECONDS
          , CASE WHEN kvi_adj.ADJ_START_TS IS NULL THEN 0
                 ELSE kvi_adj.ADJ_DURATION
            END AS ADJ_SECONDS
          , kvi.GOAL_SECONDS
          , kvi.BREAK_SECONDS
          , kvi.PERFORMANCE AS PERF
          , kvi.EXPECTED_PERF
          , kvi.CUST_NBR
          , kvi.ROUTE_NBR
          , kvi.BLENDED_COST
          , CASE WHEN kvi_adj.ADJ_START_TS IS NULL THEN 0
                 ELSE kvi_adj.ADJ_BLENDED_COST
            END AS ADJ_BLENDED_COST
          , kvi.BASE_PF_D
          , kvi.TOTAL_CASES
          , kvi.TOTAL_CUBES
          , kvi.TOTAL_WEIGHT
          , kvi.TOTAL_PALLETS
          , kvi.DISCRETE_FLG
          , kvi.DIRECT_FLG
          , kvi.MEASURED_FLG
          , kvi.ORDER_QTY
          , kvi.EXT_KVI_GOAL_SECONDS
          , kvi.EXT_DISCRETE_GOAL_SECONDS
          , kvi.INT_KVI_GOAL_SECONDS
          , kvi.INT_DISCRETE_GOAL_SECONDS
          , kvi.MACH_KVI_GOAL_SECONDS
          , kvi.MACH_DISCRETE_GOAL_SECONDS
          , kvi.EXT_ASSIGNMENT_SECONDS
          , kvi.EXT_ORDER_SECONDS
          , kvi.INT_ASSIGNMENT_SECONDS
          , kvi.INT_ORDER_SECONDS
          , kvi.MACH_ASSIGNMENT_SECONDS
          , kvi.MACH_ORDER_SECONDS
          , kvi.PICK_ERROR_QTY
          , kvi.PICK_ERROR_COST
          , kvi.SRC_ID
          , '{{ env_var(env_user) }}' AS MODIFIED_USER_ID
          , '{{ env_var(env_user) }}' AS LAST_MODIFIED_USER_ID
       FROM {{ ref('VW_KVI_SUMMARY') }} AS kvi
  LEFT JOIN {{ ref('VW_KVI_ADJUSTMENTS') }} AS kvi_adj
         ON kvi.KVI_SUMMARY_ID = kvi_adj.KVI_SUMMARY_ID
        AND kvi.EMPLOYEE_ID = kvi_adj.EMPLOYEE_ID
        AND kvi.JOB_CODE_ID = kvi_adj.JOB_CODE_ID
        AND kvi.ASSIGN_NBR = kvi_adj.ASSIGN_NBR

        {% if is_incremental() %}

        where SRC_LAST_UPDATE_DT > '{{ get_max_last_upd() }}'

        {% endif %}
    )

/* Outcome */
     SELECT *
       FROM SOURCE