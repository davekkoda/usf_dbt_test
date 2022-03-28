{% set nm_tbl = selcet_dev_db('GOLD_RED_PRAIRIE') %} -- name of raw table

WITH source -- the CTE view name
	AS(
        SELECT *
        FROM {{ source('{{ nm_tbl }}', 'WORKCATEGORY') }}
        WHERE WCT_NAME != '<none>' LIMIT 100
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
