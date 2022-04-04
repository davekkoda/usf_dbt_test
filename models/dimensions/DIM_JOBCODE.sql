WITH source -- the CTE view name
	AS(
        SELECT
            {{ dbt_utils.surrogate_key(['WH_CD', 'JOBCODE_ID', 'SRC_ID']) }} AS DIM_JOBCODE_PK
            , *
        FROM {{ ref('INT_JOBCODE') }}
        ORDER BY DIM_JOBCODE_PK
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
