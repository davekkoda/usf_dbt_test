{{ config(
    enabled=false,
    materialized='view'
)}}

WITH
     SOURCE AS (SELECT DIM_DATE_PK
                     , CLNDR_DT
                     , CLNDR_DAY_NM
                     , CLNDR_MTH_NM
                     , CLNDR_YR
                     , CLNDR_DAY_OF_WK
                     , CLNDR_DAY_OF_MTH
                     , CLNDR_WK_OF_MTH
                     , CLNDR_DAY_OF_YR
                     , CLNDR_WK_OF_YR
                     , CLNDR_MTH_OF_YR
                     , CLNDR_QTR_OF_YR
                     , CLNDR_HALF_OF_YR
                     , CLNDR_YR_QTR
                     , CLNDR_YR_MTH
                     , FISC_YR
                     , FISC_DAY_OF_YR
                     , FISC_DAY_NM
                     , FISC_WK_OF_YR
                     , FISC_WK_OF_PRD
                     , FISC_MTH_OF_YR
                     , FISC_MTH_NM
                     , FISC_QTR_OF_YR
                     , FISC_HALF_OF_YR
                     , FISC_YR_QTR
                     , FISC_YR_MTH
                     , FISC_YR_WK
                     , FISC_WKS_THIS_PD
                     , CLNDR_DT_YRAGO
                     , FISC_PRD_NM
                     , GRG_CLNDR_DT_YRAGO
                     , CLNDR_WK_STRT_DT
                     , CLNDR_WK_END_DT
                     , CLNDR_WK_NM
                     , CLNDR_YR_WK
                     , CLNDR_MTH_NM_YR
                     , GRG_CLNDR_YR_MTH_YRAGO
                     , FST_BUS_DT_NEXT_MONTH
                     , CLNDR_DT_WKAGO
                  FROM {{ref ("DIM_DATE")}})
/* Outcome */
     SELECT *
       FROM SOURCE