{{ config(materialized='incremental', unique_key='DIM_MARKET_PK') }}

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
          {{ surrogate_key_int(['EMPLOYEE_ID']) }} AS DIM_EMPLOYEE_SK
          , EMP_NBR AS WORKDAY_ID
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
          , CURRENT_DATE() AS LAST_UPDATE_DT
          , '{{ env_var(env_user) }}' AS MODIFIED_USER_ID
          , '{{ env_var(env_user) }}' AS LAST_MODIFIED_USER_ID
       FROM {{ ref('VW_EMPLOYEE') }}
    )

SELECT * FROM source -- from the CTE view build a new reference with this filename
