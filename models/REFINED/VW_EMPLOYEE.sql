{{ config(materialized='view') }}

WITH source -- the CTE view name
	AS(
     SELECT EMP_NBR AS WORKDAY_ID
          , NETWORK_ID
          , BRNCH_CD AS WAREHOUSE_ID
          , DEPT_ID
          , DEPT_NM
          , FIRST_NAME AS FIRST_NM
          , LAST_NAME AS LAST_NM
          , MIDDLE_INITIAL AS MIDDLE_INTL
          , EMP_EMAIL
          , EMP_WORK_PHONE
          , LOCATION_ID
          , BU_NAME
          , LOCATION_NAME
          , EMP_TYPE
          , PAY_TYPE
          , TIME_TYPE
          , POSITION_ID
          , MANAGER_ID
          , JOB_TITLE
          , JOB_FAM_DESC AS JOB_NM
          , JOB_FNCTN_LNG_DESC AS JOB_DESC
          , WORK_COMP_CD
          , WORK_COMP_DESC
          , HIRE_DT
          , SRVC_DT
          , TRMNTN_DT
          , EEO_JOB_GRP AS JOB_GROUP_ID
          , EEO_JOB_GRP_DESC AS JOB_GROUP_DESC
          , COMP_GRADE
          , PAY_GROUP AS PAY_GROUP_CD
          , GL_EXPENSE
          , EMP_STATUS
        FROM {{ source('GOLD_WORKDAY', 'EMPLOYEE') }}
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
