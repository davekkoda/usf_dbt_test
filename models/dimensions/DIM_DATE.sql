{{ config(materialized='incremental', unique_key='DIM_DATE_PK') }}

WITH source -- the CTE view name
	AS(
        SELECT
            TO_NUMBER(TO_CHAR(TO_DATE(CLNDR_DT),'YYYYMMDD'))AS DIM_DATE_PK
            , *
            , CURRENT_DATE() AS LAST_UPDATE_DT
            ,'{{ env_var('SNOW_USFBM_USERNAME_DEV') }}' AS MODIFIED_USER_ID
            ,'{{ env_var('SNOW_USFBM_USERNAME_DEV') }}' AS LAST_MODIFIED_USER_ID
        FROM {{ ref('INT_TIME_CORP') }}
        ORDER BY DIM_DATE_PK
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
