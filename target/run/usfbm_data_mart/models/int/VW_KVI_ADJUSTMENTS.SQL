
  create or replace  view CDW_DEV.SANDBOX_W846026.VW_KVI_ADJUSTMENTS
  
    
    
(
  
    
      ADJ_DURATION
    
    , 
  
    
      ADJ_INT_ID
    
    , 
  
    
      JOBCODE_ID
    
    , 
  
    
      WH_CD
    
    , 
  
    
      WCT_ID
    
    , 
  
    
      KVISUMMARY_ID
    
    , 
  
    
      SRC_ID
    
    , 
  
    
      LAST_UPD_TMST_SRC
    
    , 
  
    
      INS_USER_ID
    
    , 
  
    
      LAST_UPD_USER_ID
    
    , 
  
    
      REPORT_DT
    
    
  
)

   as (
    
WITH source -- the CTE view name
	AS(
        SELECT ADJ.ADJ_DURATION 
        ,ADJ.ADJ_INT_ID
        ,ADJ.JOBCODEINTID AS JOBCODE_ID
        ,ADJ.WH_ID AS WH_CD
        ,ADJ.WCT_INT_ID AS WCT_ID
        ,ADJ.KVISUMMARYINTID AS KVISUMMARY_ID
        ,ADJ.SRC_ID
        ,ADJ.LAST_UPD_DT::TIMESTAMP AS LAST_UPD_TMST_SRC
        ,ADJ.INS_USER_ID
        ,ADJ.LAST_UPD_USER_ID
        ,ADJ.REPORT_DATE AS REPORT_DT
        FROM GOLD_DEV.RED_PRAIRIE.KVI_ADJUSTMENTS AS ADJ

    )
SELECT * FROM source -- from the CTE view build a new reference with this filename
  );
