{{ config(materialized='view') }}

WITH source -- the CTE view name
	AS(
     SELECT WFCTOTALID AS WFC_TOTAL_ID
          , TIMESHEETITEMID AS TIMESHEET_ID
          , APPLYDTM AS DATE_ID
          , EMPLOYEEID AS EMPLOYEE_ID
          , PAYCODEID AS PAY_CODE_ID
          , LABORACCTID AS LABOR_ACCT_ID
          , STARTDTM AS START_TMST
          , ENDDTM AS END_TMST
          , DURATIONSECSQTY AS DURATION_SECS_QTY 
          , STIMEZONEID AS TIME_ZONE_ID
          , WAGEAMT AS WAGE_AMT
          , MONEYAMT AS MONEY_AMT
        FROM {{ source('GOLD_KRONOS', 'WFCTOTAL') }}
    )

/* Outcome */
     SELECT *
       FROM SOURCE