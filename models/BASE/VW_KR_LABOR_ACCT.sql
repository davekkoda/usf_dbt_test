{{ config(materialized='view') }}

WITH source -- the CTE view name
	AS(
     SELECT LABORACCTID AS LABOR_ACCT_ID
          , LABORLEV6ID AS JOB_ID
          , LABORLEV6NM AS JOB_CD
          , LABORLEV6DSC AS JOB_DSC
          , LABORLEV5ID AS DEPT_ID
          , LABORLEV5NM AS DEPT_CD
          , LABORLEV5DSC AS DEPT_DSC
          , LABORLEV3ID AS REGION_ID
          , LABORLEV3NM AS REGION_CD
          , LABORLEV3DSC AS REGION_DSC
          , LABORLEV4ID AS DEPT_ID
          , LABORLEV4NM AS DEPT_CD
          , LABORLEV4DSC AS DEPT_DSC
          , UPDATEDTM AS CDW_UPD_TS
        FROM {{ source('GOLD_KRONOS', 'LABORACCT') }}
    )

/* Outcome */
     SELECT *
       FROM SOURCE