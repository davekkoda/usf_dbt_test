{{ config(materialized='incremental', unique_key='DIM_MARKET_PK') }}

WITH source -- the CTE view name
	AS(
        SELECT DISTINCT
            {{ dbt_utils.surrogate_key(['WH_CD', 'DIV_ID']) }} AS DIM_MARKET_PK
            , *
        FROM {{ ref('INT_DIV_CORP') }}
        WHERE IS_ACT = 1
        ORDER BY DIM_MARKET_PK
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
