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
        SELECT 
          {{ surrogate_key_int(['LABOR_ACCT_ID']) }} AS DIM_LABOR_ACCT_SK
          , la.LABOR_ACCT_ID
          , la.JOB_ID
          , la.JOB_CD
          , la.JOB_DSC
          , la.DEPT_ID
          , la.DEPT_CD
          , la.DEPT_DSC
          , la.REGION_ID
          , la.REGION_CD
          , la.REGION_DSC
          , la.DEPT_ID
          , la.DEPT_CD
          , la.DEPT_DSC
          , CURRENT_DATE() AS LAST_UPDATE_DT
          , '{{ env_var(env_user) }}' AS MODIFIED_USER_ID
          , '{{ env_var(env_user) }}' AS LAST_MODIFIED_USER_ID
       FROM {{ ref('VW_LABOR_ACCT') }} la
    )

/* Outcome */
     SELECT *
       FROM SOURCE