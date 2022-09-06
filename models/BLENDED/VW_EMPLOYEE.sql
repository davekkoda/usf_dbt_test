{{ config(materialized='incremental', unique_key='DIM_MARKET_PK') }}

{% if target.name == 'dev' %}
{%  set env_user = "SNOW_USFBM_USERNAME_DEV" %}
{% elif target.name == 'qa' %}
{% set env_user = "SNOW_USFBM_USERNAME_QA" %}
{% elif target.name == 'prod' %}
{% set env_user = "SNOW_USFBM_USERNAME_PROD" %}
{% endif %}

WITH workday -- the CTE view name
	AS(
     SELECT EMP_NB
          , NETWORK_ID
          , MARKET_ID
          , DEPT_ID
          , DEPT_NM
          , FIRST_NM
          , LAST_NM
          , MIDDLE_INTL
          , EMP_EMAIL
          , EMP_WORK_PHONE
          , LOCATION_ID
          , BU_NM
          , LOCATION_NM
          , EMP_TYPE
          , PAY_TYPE
          , TIME_TYPE
          , POSITION_ID
          , MANAGER_ID
          , JOB_TITLE
          , JOB_NM
          , JOB_DESC
          , WORK_COMP_CD
          , WORK_COMP_DESC
          , HIRE_DT
          , SRVC_DT
          , TRMNTN_DT
          , JOB_GROUP_ID
          , JOB_GROUP_DESC
          , COMP_GRADE
          , PAY_GROUP_CD
          , GL_EXPENSE
          , EMP_STATUS
       FROM {{ ref('VW_WD_EMPLOYEE') }}
    ),

WITH red_prairie -- the CTE view name
	AS(
     SELECT EMP_NB
          , ADR_ID
          , USR_ID
          , LOGIN_ID
          , USR_STATUS
          , ACCOUNT_EXPIRE_DT
          , SUPERVISOR_ID
          , EMP_COST
          , HIRE_DT
          , CLIENT_ID
          , SUPER_USR_FLG
          , INCENTIVE_FLG
          , DIFFERENTIAL_FLG
          , UNMEASURED_FLG
          , PAYROLL_FLG
          , SRC_ID
       FROM {{ ref('VW_RP_LES_USR_ATH') }} 
    ),

WITH kronos -- the CTE view name
	AS(
     SELECT PERSON_ID
          , EMP_NB
          , FIRST_NM
          , LAST_NM
          , MIDDLE_INTL
          , FULL_NM
          , HIRE_DT
          , BIRTH_DT
          , FTEPCT
       FROM {{ ref('VW_KR_PERSON') }} 
    ),

WITH source 
     AS(
     SELECT wd.EMP_NB
          , kr.PERSON_ID AS KRONOS_ID
          , rp.ADR_ID AS RP_ADR_ID
          , rp.USR_ID AS RP_USR_ID
          , rp.LOGIN_ID AS RP_LOGIN_ID
          , wd.NETWORK_ID
          , wd.MARKET_ID
          , wd.DEPT_ID
          , wd.DEPT_NM
          , wd.FIRST_NM
          , wd.LAST_NM
          , wd.MIDDLE_INTL
          , kr.FULL_NM
          , wd.EMP_EMAIL
          , wd.EMP_WORK_PHONE
          , kr.BIRTH_DT
          , wd.LOCATION_ID
          , wd.BU_NM
          , wd.LOCATION_NM
          , wd.HIRE_DT AS WD_HIRE_DT
          , kr.HIRE_DT AS KR_HIRE_DT
          , rp.MC_EMP_HIRE_DATE AS RP_HIRE_DT
          , wd.EMP_TYPE
          , wd.PAY_TYPE
          , wd.TIME_TYPE
          , wd.POSITION_ID
          , rp.MC_SUPERVISOR_ID
          , wd.MANAGER_ID
          , wd.JOB_TITLE
          , wd.JOB_NM
          , wd.JOB_DESC
          , wd.WORK_COMP_CD
          , wd.WORK_COMP_DESC
          , rp.MC_EMP_COST
          , rp.CLIENT_ID
          , rp.SUPER_USR_FLG
          , rp.INCENTIVE_FLG
          , rp.DIFFERENTIAL_FLG
          , rp.UNMEASURED_FLG
          , rp.PAYROLL_FLG
          , kr.FTEPCT
          , wd.SRVC_DT
          , wd.TRMNTN_DT
          , wd.JOB_GROUP_ID
          , wd.JOB_GROUP_DESC
          , wd.COMP_GRADE
          , wd.PAY_GROUP_CD
          , wd.GL_EXPENSE
          , wd.EMP_STATUS
          , rp.SUPER_USR_FLG
          , rp.INCENTIVE_FLG
          , rp.DIFFERENTIAL_FLG
          , rp.UNMEASURED_FLG
          , rp.PAYROLL_FLG
          , rp.SRC_ID
       FROM workday wd
       JOIN kronos kr
         ON kr.EMP_NB = wd.EMP_NB
       JOIN red_prairie rp
         ON rp.EMP_NB= kr.EMP_NB
   ORDER BY EMP_NB
     )

/* Outcome */
     SELECT *
       FROM SOURCE