WITH source -- the CTE view name
	AS(
        SELECT
            TO_NUMBER(TO_CHAR(TO_DATE(CLNDR_DT),'YYYYMMDD'))AS DATE_SK
            , CLNDR_DT
            , CLNDR_DAY_NM
            , CLNDR_MTH_NM
            , CLNDR_YR::INTEGER
            , CLNDR_DAY_OF_WK::INTEGER
            , CLNDR_DAY_OF_MTH
            , CLNDR_WK_OF_MTH
            , CLNDR_DAY_OF_YR::INTEGER
            , CLNDR_WK_OF_YR
            , CLNDR_MTH_OF_YR
            , CLNDR_QTR_OF_YR
            , CLNDR_HALF_OF_YR
            , CLNDR_YR_QTR
            , CLNDR_YR_MTH::INTEGER
            , FISC_YR
            , FISC_DAY_OF_YR::INTEGER
            , FISC_DAY_NM
            , FISC_WK_OF_YR::INTEGER
            , FISC_WK_OF_PRD::INTEGER
            , FISC_MTH_OF_YR
            , FISC_MTH_NM
            , FISC_QTR_OF_YR
            , FISC_HALF_OF_YR
            , FISC_YR_QTR
            , FISC_YR_MTH
            , FISC_YR_WK
            , FISC_WKS_THIS_PD
            , TM_SK_YRAGO
            , CLNDR_DT_YRAGO
            , FISC_PRD_NM
            , GRG_CLNDR_DT_YRAGO
            , CLNDR_WK_STRT_DT
            , CLNDR_WK_END_DT
            , CLNDR_WK_NM
            , CLNDR_YR_WK
            , CLNDR_MTH_NM_YR
            , GRG_CLNDR_YR_MTH_YRAGO
            , LIC_WED_DT
            , LIC_THURS_DT
            , FST_BUS_DT_NEXT_MONTH
            , ORD_GD_GEN_DT
            , MON_AFTER_LIC_THURS_DT
            , CLNDR_DT_WKAGO
        FROM {{ source('GOLD_XDMADM', 'TIME_CORP') }}
    )
SELECT * FROM source -- from the CTE view build a new reference with this filename