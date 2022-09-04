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
          , emp.EMP_NB
          , emp.NETWORK_ID
          , emp.MARKET_ID
          , emp.DEPT_ID
          , emp.DEPT_NM
          , emp.FIRST_NM
          , emp.LAST_NM
          , emp.MIDDLE_INTL
          , emp.EMP_EMAIL
          , emp.EMP_WORK_PHONE
          , emp.LOCATION_ID
          , emp.BU_NM
          , emp.LOCATION_NM
          , emp.EMP_TYPE
          , emp.PAY_TYPE
          , emp.TIME_TYPE
          , emp.POSITION_ID
          , emp.MANAGER_ID
          , emp.JOB_TITLE
          , emp.JOB_NM
          , emp.JOB_DESC
          , emp.WORK_COMP_CD
          , emp.WORK_COMP_DESC
          , emp.HIRE_DT
          , emp.SRVC_DT
          , emp.TRMNTN_DT
          , emp.JOB_GROUP_ID
          , emp.JOB_GROUP_DESC
          , emp.COMP_GRADE
          , emp.PAY_GROUP_CD
          , emp.GL_EXPENSE
          , emp.EMP_STATUS
          , CURRENT_DATE() AS LAST_UPDATE_DT
          , '{{ env_var(env_user) }}' AS MODIFIED_USER_ID
          , '{{ env_var(env_user) }}' AS LAST_MODIFIED_USER_ID
       FROM {{ ref('VW_EMPLOYEE') }} as emp
    )

/* Outcome */
     SELECT *
       FROM SOURCE