WITH source -- the CTE view name
	AS(
        SELECT
            {{ dbt_utils.surrogate_key(['BRNCH_CD']) }} AS DIM_MARKET_SK
            , *
        WHEN INACT_DT IS NULL THEN 1
        ELSE 0 END AS IS_ACT
        FROM {{ ref('INT_DIM_MARKET') }}
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
