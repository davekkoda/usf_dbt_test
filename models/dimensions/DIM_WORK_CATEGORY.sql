WITH source -- the CTE view name
	AS(
<<<<<<< HEAD
        SELECT
            {{ dbt_utils.surrogate_key(['WH_ID', 'WCT_INT_ID', 'SRC_ID']) }} AS DIM_WORK_CATEGORY_SK
            , *
        FROM {{ source('GOLD_RED_PRAIRIE', 'WORKCATEGORY') }}
        WHERE WCT_NAME != '<none>'
=======
        SELECT *
        FROM {{ source('GOLD_RED_PRAIRIE', 'WORKCATEGORY') }}
        WHERE WCT_NAME != '<none>' LIMIT 100
>>>>>>> main
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename