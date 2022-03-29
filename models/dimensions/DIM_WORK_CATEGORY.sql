WITH source -- the CTE view name
	AS(
        SELECT
            {{ dbt_utils.surrogate_key(['WH_ID', 'WCT_INT_ID', 'SRC_ID']) }} AS DIM_WORK_CATEGORY_SK
            , *
        FROM {{ ref('INT_DIM_WORK_CATEGORY') }}
        WHERE WCT_NAME != '<none>'
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename