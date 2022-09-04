{{ config(materialized='view') }}

WITH source -- the CTE view name
	AS(
     SELECT
       FROM {{ source('GOLD_RED_PRAIRIE'
                           , 'LES_USR_ATH') }}
    )

/* Outcome */
     SELECT *
       FROM SOURCE