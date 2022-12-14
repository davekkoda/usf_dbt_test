begin;
    
        
            
            
        
    

    

    merge into CDW_DEV.SANDBOX_W846026.DIM_WORK_CATEGORY as DBT_INTERNAL_DEST
        using CDW_DEV.SANDBOX_W846026.DIM_WORK_CATEGORY__dbt_tmp as DBT_INTERNAL_SOURCE
        on 
                DBT_INTERNAL_SOURCE.DIM_WORK_CATEGORY_PK = DBT_INTERNAL_DEST.DIM_WORK_CATEGORY_PK
            

    
    when matched then update set
        "DIM_WORK_CATEGORY_SK" = DBT_INTERNAL_SOURCE."DIM_WORK_CATEGORY_SK","WCT_ID" = DBT_INTERNAL_SOURCE."WCT_ID","WH_CD" = DBT_INTERNAL_SOURCE."WH_CD","WCT_NM" = DBT_INTERNAL_SOURCE."WCT_NM","SRC_ID" = DBT_INTERNAL_SOURCE."SRC_ID","LAST_UPDATE_DT" = DBT_INTERNAL_SOURCE."LAST_UPDATE_DT","MODIFIED_USER_ID" = DBT_INTERNAL_SOURCE."MODIFIED_USER_ID","LAST_MODIFIED_USER_ID" = DBT_INTERNAL_SOURCE."LAST_MODIFIED_USER_ID"
    

    when not matched then insert
        ("DIM_WORK_CATEGORY_SK", "WCT_ID", "WH_CD", "WCT_NM", "SRC_ID", "LAST_UPDATE_DT", "MODIFIED_USER_ID", "LAST_MODIFIED_USER_ID")
    values
        ("DIM_WORK_CATEGORY_SK", "WCT_ID", "WH_CD", "WCT_NM", "SRC_ID", "LAST_UPDATE_DT", "MODIFIED_USER_ID", "LAST_MODIFIED_USER_ID")

;
    commit;