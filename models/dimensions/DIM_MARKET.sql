WITH source -- the CTE view name
	AS(
        SELECT
            {{ dbt_utils.surrogate_key(['WH_CD']) }} AS DIM_MARKET_PK
            , *
        FROM {{ ref('INT_DIV_CORP') }}
        ORDER BY DIM_MARKET_PK
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
