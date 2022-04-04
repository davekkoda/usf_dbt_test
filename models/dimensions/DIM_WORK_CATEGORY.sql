WITH source -- the CTE view name
	AS(
        SELECT
            {{ dbt_utils.surrogate_key(['WH_CD', 'WCT_ID', 'SRC_ID']) }} AS DIM_WORK_CATEGORY_PK
            , *
        FROM {{ ref('INT_WORKCATEGORY') }}
        WHERE WCT_NAME != '<none>'
        ORDER BY DIM_WORK_CATEGORY_PK
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename