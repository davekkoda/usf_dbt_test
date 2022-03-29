WITH source -- the CTE view name
	AS(
        SELECT
            TO_NUMBER(TO_CHAR(TO_DATE(CLNDR_DT),'YYYYMMDD'))AS DATE_SK
            , *
        FROM {{ ref('INT_DIM_DATE') }}
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
