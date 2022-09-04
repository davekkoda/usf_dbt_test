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
          {{ surrogate_key_int(['wd.EMPLOYEE_ID']) }} AS DIM_EMPLOYEE_SK
          , wd.EMP_NB
          , wd.NETWORK_ID
          , wd.MARKET_ID
          , wd.DEPT_ID
          , wd.DEPT_NM
          , wd.FIRST_NM
          , wd.LAST_NM
          , wd.MIDDLE_INTL
          , wd.EMP_EMAIL
          , wd.EMP_WORK_PHONE
          , wd.LOCATION_ID
          , wd.BU_NM
          , wd.LOCATION_NM
          , wd.EMP_TYPE
          , wd.PAY_TYPE
          , wd.TIME_TYPE
          , wd.POSITION_ID
          , wd.MANAGER_ID
          , wd.JOB_TITLE
          , wd.JOB_NM
          , wd.JOB_DESC
          , wd.WORK_COMP_CD
          , wd.WORK_COMP_DESC
          , wd.HIRE_DT
          , wd.SRVC_DT
          , wd.TRMNTN_DT
          , wd.JOB_GROUP_ID
          , wd.JOB_GROUP_DESC
          , wd.COMP_GRADE
          , wd.PAY_GROUP_CD
          , wd.GL_EXPENSE
          , wd.EMP_STATUS
          , CURRENT_DATE() AS LAST_UPDATE_DT
          , '{{ env_var(env_user) }}' AS MODIFIED_USER_ID
          , '{{ env_var(env_user) }}' AS LAST_MODIFIED_USER_ID
       FROM {{ ref('VW_WD_EMPLOYEE') }} as wd
    )

/* Outcome */
     SELECT *
       FROM SOURCE