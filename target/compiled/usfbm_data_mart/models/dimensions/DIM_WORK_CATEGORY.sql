WITH source -- the CTE view name
	AS(
        SELECT *
        FROM GOLD.RED_PRAIRIE.WORKCATEGORY
        WHERE WCT_NAME != '<none>' LIMIT 100
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename