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
          , *
       FROM {{ ref('VW_KR_LABOR_ACCT') }}
   ORDER BY LABOR_ACCT_ID
    )

/* Outcome */
     SELECT *
       FROM SOURCE