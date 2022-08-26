begin;
    
        
            
            
        
    

    

    merge into CDW_DEV.SANDBOX_W846026.DIM_MARKET as DBT_INTERNAL_DEST
        using CDW_DEV.SANDBOX_W846026.DIM_MARKET__dbt_tmp as DBT_INTERNAL_SOURCE
        on 
                DBT_INTERNAL_SOURCE.DIM_MARKET_PK = DBT_INTERNAL_DEST.DIM_MARKET_PK
            

    
    when matched then update set
        "DIM_MARKET_SK" = DBT_INTERNAL_SOURCE."DIM_MARKET_SK","WH_CD" = DBT_INTERNAL_SOURCE."WH_CD","RGN_CD" = DBT_INTERNAL_SOURCE."RGN_CD","DIV_TYP_CD" = DBT_INTERNAL_SOURCE."DIV_TYP_CD","ZIP_CD" = DBT_INTERNAL_SOURCE."ZIP_CD","DIV_ID" = DBT_INTERNAL_SOURCE."DIV_ID","CONV_TO_DIV_NB" = DBT_INTERNAL_SOURCE."CONV_TO_DIV_NB","PRCS_SYS_NM" = DBT_INTERNAL_SOURCE."PRCS_SYS_NM","DIV_NM" = DBT_INTERNAL_SOURCE."DIV_NM","DIV_NM_CD_NB" = DBT_INTERNAL_SOURCE."DIV_NM_CD_NB","RGN_NM" = DBT_INTERNAL_SOURCE."RGN_NM","AREA_NM" = DBT_INTERNAL_SOURCE."AREA_NM","SC_RGN_NM" = DBT_INTERNAL_SOURCE."SC_RGN_NM","CMPNY_DESC" = DBT_INTERNAL_SOURCE."CMPNY_DESC","INACT_DT" = DBT_INTERNAL_SOURCE."INACT_DT","IS_ACT" = DBT_INTERNAL_SOURCE."IS_ACT","LAST_UPDATE_DT" = DBT_INTERNAL_SOURCE."LAST_UPDATE_DT","MODIFIED_USER_ID" = DBT_INTERNAL_SOURCE."MODIFIED_USER_ID","LAST_MODIFIED_USER_ID" = DBT_INTERNAL_SOURCE."LAST_MODIFIED_USER_ID"
    

    when not matched then insert
        ("DIM_MARKET_SK", "WH_CD", "RGN_CD", "DIV_TYP_CD", "ZIP_CD", "DIV_ID", "CONV_TO_DIV_NB", "PRCS_SYS_NM", "DIV_NM", "DIV_NM_CD_NB", "RGN_NM", "AREA_NM", "SC_RGN_NM", "CMPNY_DESC", "INACT_DT", "IS_ACT", "LAST_UPDATE_DT", "MODIFIED_USER_ID", "LAST_MODIFIED_USER_ID")
    values
        ("DIM_MARKET_SK", "WH_CD", "RGN_CD", "DIV_TYP_CD", "ZIP_CD", "DIV_ID", "CONV_TO_DIV_NB", "PRCS_SYS_NM", "DIV_NM", "DIV_NM_CD_NB", "RGN_NM", "AREA_NM", "SC_RGN_NM", "CMPNY_DESC", "INACT_DT", "IS_ACT", "LAST_UPDATE_DT", "MODIFIED_USER_ID", "LAST_MODIFIED_USER_ID")

;
    commit;