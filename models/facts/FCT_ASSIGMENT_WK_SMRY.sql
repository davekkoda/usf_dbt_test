{% if target.name == 'dev' %}
{%  set env_user = "SNOW_USFBM_USERNAME_DEV" %}
{% elif target.name == 'qa' %}
{% set env_user = "SNOW_USFBM_USERNAME_QA" %}
{% elif target.name == 'prod' %}
{% set env_user = "SNOW_USFBM_USERNAME_PROD" %}
{% endif %}
WITH
     SOURCE AS (SELECT
                    DAT.CLNDR_WK_STRT_DT
                     , FCT.DIM_MARKET_SK
                     , FCT.DIM_JOBCODE_SK
                     ,(CASE WHEN WCT.WCT_NAME = 'Receiving' THEN 'DAY'
                            WHEN WCT.WCT_NAME = 'Putaway' THEN 'DAY'
                            WHEN WCT.WCT_NAME= 'Selection Shipped' OR WCT.WCT_NAME = 'Selection Merged'
                            THEN 'NIGHT'
                            WHEN WCT.WCT_NAME = 'Loading' THEN 'NIGHT'
                            WHEN CAST(SUBSTRING(FCT.START_TIME, 12, 2) AS int) < 17
                             AND CAST(SUBSTRING(FCT.START_TIME, 12, 2) AS int) >=5
                            THEN 'DAY'
                            ELSE 'NIGHT'
                       END) AS DAY_NIGHT
                     , COALESCE(SUM(CASE WHEN FCT.ISMEASURED = 1
                                          AND FCT.DIRECT = 1 THEN FCT.ACTUAL_SECONDS
                                    END) /3600, 0) AS MEASURED_DIRECT
                     , COALESCE(SUM(CASE WHEN FCT.ISMEASURED = 1
                                          AND FCT.DIRECT = 0 THEN FCT.ACTUAL_SECONDS
                                    END) /3600, 0) AS MEASURED_INDIRECT
                     , COALESCE(SUM(CASE WHEN FCT.ISMEASURED = 0
                                          AND FCT.DIRECT = 1
                                          AND JOB.JOBCODE_CD NOT IN('BREAK', 'IBREAK', 'ZIBREAK', 'ILUNCH', 'LUNCH', 'INPBRE')
                                         THEN FCT.ACTUAL_SECONDS
                                    END) /3600, 0) AS UNMEASURED_DIRECT
                     , COALESCE(SUM(CASE WHEN FCT.ISMEASURED = 0
                                          AND FCT.DIRECT = 0
                                          AND JOB.JOBCODE_CD NOT IN('BREAK', 'IBREAK', 'ZIBREAK', 'ILUNCH', 'LUNCH', 'INPBRE')
                                         THEN FCT.ACTUAL_SECONDS
                                    END) /3600, 0) AS UNMEASURED_INDIRECT
                     , COALESCE(SUM(CASE WHEN JOB.JOBCODE_CD IN('ILUNCH', 'LUNCH', 'INPBRE')
                                         THEN FCT.ACTUAL_SECONDS
                                    END) /3600, 0) AS UNPAID_BREAK
                     , COALESCE(SUM(CASE WHEN JOB.JOBCODE_CD IN('BREAK', 'IBREAK', 'ZIBREAK')
                                         THEN FCT.ACTUAL_SECONDS
                                    END) /3600, 0) AS PAID_BREAK
                     , COALESCE(SUM(CASE WHEN FCT.DIRECT = 1
                                          AND JOB.JOBCODE_CD NOT IN('BREAK', 'IBREAK', 'ZIBREAK', 'ILUNCH', 'LUNCH', 'INPBRE')
                                         THEN FCT.ADJ_DURATION
                                    END) /3600, 0) AS INSERTED_DIRECT
                     , COALESCE(SUM(CASE WHEN FCT.DIRECT = 0
                                          AND JOB.JOBCODE_CD NOT IN('BREAK', 'IBREAK', 'ZIBREAK', 'ILUNCH', 'LUNCH', 'INPBRE')
                                         THEN FCT.ADJ_DURATION
                                    END) /3600, 0) AS INSERTED_INDIRECT
                     ,(MEASURED_DIRECT + MEASURED_INDIRECT + UNMEASURED_DIRECT + UNMEASURED_INDIRECT + PAID_BREAK + UNPAID_BREAK + INSERTED_DIRECT + INSERTED_INDIRECT)/3600 AS TOTAL_HOURS
                     , COALESCE(SUM(FCT.GOAL_SECONDS) /3600, 0) AS GOAL_HOURS
                     , CURRENT_DATE() AS LAST_UPDATE_DT
                     , '{{ env_var(env_user) }}' AS MODIFIED_USER_ID
                     , '{{ env_var(env_user) }}' AS LAST_MODIFIED_USER_ID
                  FROM {{ ref('FCT_ASSIGMENT') }} FCT
                  JOIN {{ ref('DIM_DATE') }} DAT
                    ON FCT.DIM_DATE_SK = DAT.DIM_DATE_SK
                  JOIN {{ ref('DIM_JOBCODE') }} JOB
                    ON FCT.DIM_JOBCODE_SK = JOB.DIM_JOBCODE_SK
                  JOIN {{ ref('DIM_WORK_CATEGORY') }} WCT
                    ON FCT.DIM_WORK_CATEGORY_SK = WCT.DIM_WORK_CATEGORY_SK
              GROUP BY DAT.CLNDR_WK_STRT_DT
                     , FCT.DIM_MARKET_SK
                     , FCT.DIM_JOBCODE_SK
                     , DAY_NIGHT
                     )
                     
/* Outcome */
     SELECT *
       FROM SOURCE

