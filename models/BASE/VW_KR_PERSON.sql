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
     SELECT PERSONNUM AS EMP_NB
          , PERSONID AS PERSON_ID
          , FIRSTNM AS FIRST_NM
          , LASTNM AS LAST_NM
          , MIDDLEINITIALNM AS MIDDLE_INTL
          , FULLNM AS FULL_NM
          , COMPANYHIREDT AS HIRE_DT
          , BIRTHDTM AS BIRTH_DT
          , FTEPCT
       FROM {{ source('GOLD_KRONOS', 'PERSON') }}
       )

/* Outcome */
     SELECT *
       FROM SOURCE