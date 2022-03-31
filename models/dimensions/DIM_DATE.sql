WITH source -- the CTE view name
	AS(
        SELECT
            TO_NUMBER(TO_CHAR(TO_DATE(CLNDR_DT),'YYYYMMDD'))AS DIM_DATE_SK
            , *
        FROM {{ ref('INT_TIME_CORP') }}
        ORDER BY DIM_DATE_SK
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
