WITH source -- the CTE view name
	AS(
        SELECT
            DIRECT::boolean AS DIRECT
            ,JOBCODEID::varchar(128) AS JOBCODE_CD
            ,WH_ID::varchar(8) AS WH_ID
            ,JOBCODEINTID::integer AS JOBCODE_ID
            ,SRC_ID::varchar(8) AS SRC_ID
        FROM {{ source('GOLD_RED_PRAIRIE', 'JOBCODE') }}
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
