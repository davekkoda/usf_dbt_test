{{ config(materialized='incremental') }}

{% if target.name == 'dev' %}
{%  set env_user = "SNOW_USFBM_USERNAME_DEV" %}
{% elif target.name == 'qa' %}
{% set env_user = "SNOW_USFBM_USERNAME_QA" %}
{% elif target.name == 'prod' %}
{% set env_user = "SNOW_USFBM_USERNAME_PROD" %}
{% endif %}

WITH kvi_summary
     AS(
     SELECT USR_ID
          , MARKET_ID
          , JOB_CODE_ID
          , JOB_CODE_NM
          , SUPERVISOR_ID
          , START_LOC_ID
          , END_LOC_ID
          , START_TS
          , END_TS
          , ACTUAL_SECONDS
          , GOAL_SECONDS
          , BREAK_SECONDS
          , PERFORMANCE
          , EXPECTED_PERF
          , CUST_NB
          , ROUTE_NB
          , BLENDED_COST
          , BASE_PF_D
          , ASSIGN_NB
          , KVI_CASES
          , KVI_PALLETS
          , VOLUME
          , PRODUCTS
          , TOTAL_CASES
          , TOTAL_CUBES
          , TOTAL_WEIGHT
          , DISCRETE_FLG
          , DIRECT_FLG
          , MEASURED_FLG
          , ORDER_QTY
          , EXT_KVI_GOAL_SECONDS
          , EXT_DISCRETE_GOAL_SECONDS
          , INT_KVI_GOAL_SECONDS
          , INT_DISCRETE_GOAL_SECONDS
          , MACH_KVI_GOAL_SECONDS
          , MACH_DISCRETE_GOAL_SECONDS
          , EXT_ASSIGNMENT_SECONDS
          , EXT_ORDER_SECONDS
          , INT_ASSIGNMENT_SECONDS
          , INT_ORDER_SECONDS
          , MACH_ASSIGNMENT_SECONDS
          , MACH_ORDER_SECONDS
          , PICK_ERROR_QTY
          , PICK_ERROR_COST
          , SRC_ID
       FROM {{ ref('VW_RP_KVI_SUMMARY') }} 
     ),

WITH kvi_adjustments
    AS(
     SELECT KVI_SUMMARY_ID
          , USR_ID
          , JOB_CODE_ID
          , ADJ_START_TS
          , ADJ_DURATION
          , ADJ_BLENDED_COST
          , ASSIGN_NB
          , SRC_ID
       FROM {{ ref('VW_RP_KVI_ADJUSTMENT') }} 
    ),
  
WITH employee
    AS(
     SELECT EMP_NB
          , RP_USR_ID
       FROM {{ ref('VW_RP_LES_USR_ATH') }}
    ),

WITH source -- the CTE view name
    AS(
     SELECT {{ surrogate_key_int(['EMP_NB']) }} AS DIM_EMPLOYEE_SK
          , emp.EMP_NB
          , kvi.RP_USR_ID
          , {{ surrogate_key_int(['MARKET_ID']) }} AS DIM_MARKET_SK
          , {{ surrogate_key_int(['JOB_CODE_ID', 'SRC_ID']) }} AS DIM_JOB_CODE_SK
          , TO_NUMBER(TO_CHAR(TO_DATE(kvi.PLAN_DATE), 'YYYYMMDD')) AS DIM_DATE_SK
          , {{ surrogate_key_int(['SUPERVISOR_ID']) }} AS SUPERVISOR_SK
          , kvi.START_LOC_ID
          , kvi.END_LOC_ID
          , kvi.START_TS
          , kvi_adj.ADJ_START_TS
          , kvi.END_TS
          , kvi.ACTUAL_SECONDS
          , CASE WHEN kvi_adj.ADJ_START_TS IS NULL THEN 0
                 ELSE kvi_adj.ADJ_DURATION
            END AS ADJ_SECONDS
          , kvi.GOAL_SECONDS
          , kvi.BREAK_SECONDS
          , kvi.PERFORMANCE
          , kvi.EXPECTED_PERF
          , kvi.CUST_NB
          , kvi.ROUTE_NB
          , kvi.BLENDED_COST
          , CASE WHEN kvi_adj.ADJ_START_TS IS NULL THEN 0
                 ELSE kvi_adj.ADJ_BLENDED_COST
            END AS ADJ_BLENDED_COST
          , kvi.BASE_PF_D
          , kvi.ASSIGN_NB
          , kvi.KVI_CASES
          , kvi.KVI_PALLETS
          , kvi.VOLUME
          , kvi.PRODUCTS
          , kvi.TOTAL_CASES
          , kvi.TOTAL_CUBES
          , kvi.TOTAL_WEIGHT
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
       FROM kvi_summary AS kvi
  LEFT JOIN kvi_adjustments AS kvi_adj
         ON kvi.KVI_SUMMARY_ID = kvi_adj.KVI_SUMMARY_ID
        AND kvi.EMPLOYEE_ID = kvi_adj.EMPLOYEE_ID
        AND kvi.JOB_CODE_ID = kvi_adj.JOB_CODE_ID
        AND kvi.ASSIGN_NB = kvi_adj.ASSIGN_NB
       JOIN employee AS emp
         ON emp.RP_USR_ID = kvi.USR_ID

        -- {% if is_incremental() %}

        -- where SRC_LAST_UPDATE_DT > '{{ get_max_last_upd() }}'

        -- {% endif %}
    )

/* Outcome */
     SELECT *
       FROM SOURCE