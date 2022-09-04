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
          , TO_NUMBER(TO_CHAR(TO_DATE(PLAN_DATE), 'YYYYMMDD')) AS DIM_DATE_SK
          , {{ surrogate_key_int(['SUPERVISOR_ID']) }} AS SUPERVISOR_SK
          , START_LOC_ID
          , END_LOC_ID
          , START_TS
          , adj.ADJ_START_TS
          , END_TS
          , ACTUAL_SECONDS
          , CASE WHEN adj.ADJ_START_TS IS NULL THEN 0
                 ELSE adj.ADJ_DURATION
            END AS ADJ_SECONDS
          , GOAL_SECONDS
          , BREAK_SECONDS
          , PERFORMANCE
          , EXPECTED_PERF
          , CUST_NB
          , ROUTE_NB
          , BLENDED_COST
          , CASE WHEN adj.ADJ_START_TS IS NULL THEN 0
                 ELSE adj.ADJ_BLENDED_COST
            END AS ADJ_BLENDED_COST
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
          , CURRENT_DATE() AS LAST_UPDATE_DT
          , '{{ env_var(env_user) }}' AS MODIFIED_USER_ID
          , '{{ env_var(env_user) }}' AS LAST_MODIFIED_USER_ID
       FROM {{ ref('VW_WH_PRODUCTIVITY') }} AS kvi

        {% if is_incremental() %}

        where SRC_LAST_UPDATE_DT > '{{ get_max_last_upd() }}'

        {% endif %}
    )

/* Outcome */
     SELECT *
       FROM SOURCE