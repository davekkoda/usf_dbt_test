WITH source -- the CTE view name
	AS(
        SELECT
            DIRECT::boolean AS DIRECT
            ,JOBCODEID::varchar(128) AS JOBCODE_DESCRIPTION
            ,WH_ID::varchar(8) AS JOBCODE_DESCRIPTION
            ,JOBCODEINTID::integer AS JOBCODE_ID
            ,SRC_ID::varchar(8) AS SRC_ID
        FROM {{ source('GOLD_RED_PRAIRIE', 'JOBCODE') }}
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
