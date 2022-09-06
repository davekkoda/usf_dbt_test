{{ config(materialized='incremental', unique_key='LABOR_ACCT_SK') }}

{% if target.name == 'dev' %}
{%  set env_user = "SNOW_USFBM_USERNAME_DEV" %}
{% elif target.name == 'qa' %}
{% set env_user = "SNOW_USFBM_USERNAME_QA" %}
{% elif target.name == 'prod' %}
{% set env_user = "SNOW_USFBM_USERNAME_PROD" %}
{% endif %}

WITH source -- the CTE view name
	AS(
     SELECT {{ surrogate_key_int(['LABOR_ACCT_ID']) }} AS DIM_LABOR_ACCT_SK
          , LABOR_ACCT_ID
          , JOB_ID
          , JOB_CD
          , JOB_DSC
          , DEPT_ID
          , DEPT_CD
          , DEPT_DSC
          , REGION_ID
          , REGION_CD
          , REGION_DSC
          , DEPT_ID
          , DEPT_CD
          , DEPT_DSC
       FROM {{ ref('VW_KR_LABOR_ACCT') }}
   ORDER BY LABOR_ACCT_ID
    )

/* Outcome */
     SELECT *
       FROM SOURCE