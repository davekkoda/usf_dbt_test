WITH source -- the CTE view name
	AS(
        SELECT 
            {{ dbt_utils.surrogate_key(['BRNCH_CD', 'RPRT_DT_TM']) }} AS DIM_DRIVER_SK
            , *
        FROM {{ ref('INT_DIM_DRIVER') }}
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
