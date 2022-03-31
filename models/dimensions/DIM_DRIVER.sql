WITH source -- the CTE view name
	AS(
        SELECT 
            {{ dbt_utils.surrogate_key(['WH_ID', 'RPRT_DT_TM']) }} AS DIM_DRIVER_SK
            , *
            
        FROM {{ ref('INT_DRIVER_LOG_HDR') }}
        ORDER BY DIM_DRIVER_SK
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
