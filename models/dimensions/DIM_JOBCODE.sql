{{ config(materialized='incremental', unique_key='DIM_JOBCODE_PK') }}

WITH source -- the CTE view name
	AS(
        SELECT DISTINCT
            {{ dbt_utils.surrogate_key(['WH_CD', 'JOBCODE_ID', 'SRC_ID']) }} AS DIM_JOBCODE_PK
            , *
            , CURRENT_DATE() AS LAST_UPDATE_DT
            ,'{{ env_var('SNOW_USFBM_USERNAME_DEV') }}' AS MODIFIED_USER_ID
            ,'{{ env_var('SNOW_USFBM_USERNAME_DEV') }}' AS LAST_MODIFIED_USER_ID
        FROM {{ ref('INT_JOBCODE') }}
        ORDER BY DIM_JOBCODE_PK
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
