WITH source -- the CTE view name
	AS(
        SELECT
            {{ dbt_utils.surrogate_key(['WH_ID']) }} AS DIM_MARKET_SK
            , *
        FROM {{ ref('INT_DIV_CORP') }}
        ORDER BY DIM_MARKET_SK
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
