WITH source -- the CTE view name
	AS(
        SELECT
            {{ dbt_utils.surrogate_key(['WH_ID', 'JOBCODEINTID', 'SRC_ID']) }} AS DIM_JOBCODE_SK
            ,DIRECT
            ,JOBCODEID
            ,WH_ID
            ,JOBCODEINTID
            ,SRC_ID 
        FROM {{ source('GOLD_RED_PRAIRIE', 'JOBCODE') }}
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
