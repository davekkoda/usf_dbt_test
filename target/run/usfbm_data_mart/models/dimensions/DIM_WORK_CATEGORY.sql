

      create or replace transient table BI_MODERNIZATION_POC_DEV.B3C6026.DIM_WORK_CATEGORY  as
      (WITH source -- the CTE view name
	AS(
        SELECT *
        FROM GOLD.RED_PRAIRIE.WORKCATEGORY
        WHERE WCT_NAME != '<none>' LIMIT 100
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
      );
    