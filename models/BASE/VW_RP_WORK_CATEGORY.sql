{{ config(materialized="view") }}

WITH source 
    AS (
     SELECT WORK_CATEGORY_NM AS WORK_CATEGORY_NM
          , WORK_CATEGORY_ID
       FROM {{source('GOLD_RED_PRAIRIE'
                          , 'WORKCATEGORY')}} 
    )

/* Outcome */
     SELECT *
       FROM SOURCE