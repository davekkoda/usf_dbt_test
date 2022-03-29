WITH source -- the CTE view name
	AS(
        SELECT
            {{ dbt_utils.surrogate_key(['WH_ID', 'JOBCODE_ID', 'SRC_ID']) }} AS DIM_JOBCODE_SK
            , *
        FROM {{ ref('INT_DIM_JOBCODE') }}
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
