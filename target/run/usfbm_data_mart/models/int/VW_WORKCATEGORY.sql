
  create or replace  view CDW_DEV.SANDBOX_W846026.VW_WORKCATEGORY
  
    
    
(
  
    
      WCT_ID
    
    , 
  
    
      WH_CD
    
    , 
  
    
      WCT_NM
    
    , 
  
    
      SRC_ID
    
    
  
)

   as (
    

WITH source -- the CTE view name
	AS(
        SELECT
            WCT_INT_ID::integer AS WCT_ID
            ,WH_ID::varchar(8) AS WH_CD
            ,WCT_NAME::varchar(128) as WCT_NM
            ,SRC_ID::varchar(8) as SRC_ID
        FROM GOLD_DEV.RED_PRAIRIE.WORKCATEGORY
        WHERE WCT_NAME != '<none>'
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
  );
