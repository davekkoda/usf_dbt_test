{{ config(materialized='view') }}

WITH source -- the CTE view name
	AS(
     SELECT OIDC_SUBJECT AS EMP_NB
          , ADR_ID
          , USR_ID
          , LOGIN_ID
          , USR_STS AS USR_STATUS
          , ACCT_EXPIR_DAT AS ACCOUNT_EXPIRE_DT
          , MC_SUPERVISOR_ID AS SUPERVISOR_ID
          , MC_EMP_COST AS EMP_COST
          , MC_EMP_HIRE_DATE AS HIRE_DT
          , CLIENT_ID
          , SUPER_USR_FLG
          , INCENTIVE_FLG
          , DIFFERENTIAL_FLG
          , UNMEASURED_FLG
          , PAYROLL_FLG
          , SRC_ID
       FROM {{ source('GOLD_RED_PRAIRIE', 'LES_USR_ATH') }}
    )

/* Outcome */
     SELECT *
       FROM SOURCE