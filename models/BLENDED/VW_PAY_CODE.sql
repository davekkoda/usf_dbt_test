{{ config(materialized='incremental', unique_key='DIM_PAY_CODE_SK') }}

{% if target.name == 'dev' %}
{%  set env_user = "SNOW_USFBM_USERNAME_DEV" %}
{% elif target.name == 'qa' %}
{% set env_user = "SNOW_USFBM_USERNAME_QA" %}
{% elif target.name == 'prod' %}
{% set env_user = "SNOW_USFBM_USERNAME_PROD" %}
{% endif %}

WITH source -- the CTE view name
	AS(
     SELECT {{ surrogate_key_int(['PAY_CODE_ID']) }} AS DIM_PAY_CODE_SK 
          , *
       FROM {{ ref('VW_KR_PAY_CODE') }} 
   ORDER BY PAY_CODE_ID
    )

/* Outcome */
     SELECT *
       FROM SOURCE