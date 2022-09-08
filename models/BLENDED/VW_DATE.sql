{{ config(materialized='incremental', unique_key='DIM_DATE_SK') }}

{% if target.name == 'dev' %}
{%  set env_user = "SNOW_USFBM_USERNAME_DEV" %}
{% elif target.name == 'qa' %}
{% set env_user = "SNOW_USFBM_USERNAME_QA" %}
{% elif target.name == 'prod' %}
{% set env_user = "SNOW_USFBM_USERNAME_PROD" %}
{% endif %}

WITH source -- the CTE view name
	AS(
     SELECT TO_NUMBER(TO_CHAR(TO_DATE(CLNDR_DT), 'YYYYMMDD')) AS DIM_DATE_SK
          , *
       FROM {{ ref('VW_XD_TIME_CORP') }}
   ORDER BY DIM_DATE_SK
    )

/* Outcome */
     SELECT *
       FROM SOURCE