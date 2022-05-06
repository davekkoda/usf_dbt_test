{{ config(materialized='view') }}

WITH source -- the CTE view name
	AS(
        SELECT
            WCT_INT_ID::integer AS WCT_ID
            ,WH_ID::varchar(8) AS WH_CD
            ,WCT_NAME::varchar(128) as WCT_NM
            ,SRC_ID::varchar(8) as SRC_ID
        FROM {{ source('GOLD_RED_PRAIRIE', 'WORKCATEGORY') }}
        WHERE WCT_NAME != '<none>'
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
