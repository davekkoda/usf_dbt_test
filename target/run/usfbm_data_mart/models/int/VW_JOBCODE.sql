
  create or replace  view CDW_DEV.SANDBOX_W846026.VW_JOBCODE
  
    
    
(
  
    
      DIRECT
    
    , 
  
    
      JOBCODE_CD
    
    , 
  
    
      WH_CD
    
    , 
  
    
      JOBCODE_ID
    
    , 
  
    
      SRC_ID
    
    
  
)

   as (
    

WITH source -- the CTE view name
	AS(
        SELECT
            DIRECT::boolean AS DIRECT
            ,JOBCODEID::varchar(128) AS JOBCODE_CD
            ,WH_ID::varchar(8) AS WH_CD
            ,JOBCODEINTID::integer AS JOBCODE_ID
            ,SRC_ID::varchar(8) AS SRC_ID
        FROM GOLD_DEV.RED_PRAIRIE.JOBCODE
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
  );
