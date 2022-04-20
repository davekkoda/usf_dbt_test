{{ config(
    enabled=false,
    materialized='view'
)}}

WITH
     SOURCE AS (SELECT DIM_DRIVER_PK AS "Driver Primary Key"
                     , WH_CD AS "Warehouse Code"
                     , RPRT_DT_TM::DATE AS "Report Date"
                     , DRVR_ID AS "Driver ID"
                     , DRVR_NM AS "Driver Name"
                  FROM {{ref ("DIM_DRIVER")}}
                 WHERE 1=1
                   AND RPRT_DT_TMBETWEEN DATE_TRUNC(Month, ADD_MONTHS(CURRENT_DATE, -13))
                   AND DATEADD('DAYS', -1, CURRENT_DATE))
/* Outcome */
     SELECT *
       FROM SOURCE