WITH source -- the CTE view name
	AS(
        SELECT 
            {{ dbt_utils.surrogate_key(['WH_CD', 'RPRT_DT_TM', 'DRVR_ID', 'DRVR_NM']) }} AS DIM_DRIVER_PK
            , *
            
        FROM {{ ref('INT_DRIVER_LOG_HDR') }}
        ORDER BY DIM_DRIVER_PK
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
