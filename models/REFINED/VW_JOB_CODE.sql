{{ config(materialized='view') }}

WITH source -- the CTE view name
	AS(

WITH
     CLEANED_JOB_CODE AS (SELECT ROW_NUMBER() OVER (partition by j.JOBCODEINTID, j.JOBCODEID, j.DESCRIPTION
                                                   ORDER BY j.SRC_ID) ROW_COUNT,
     j.JOBCODEINTID AS JOB_CODE_ID ,
     j.JOBCODEID AS JOB_CODE_NM ,
     j.WH_ID AS MARKET_ID ,
     CASE WHEN JOB_CODE_NM IN('LOADSTARTF', 'LOADSTARTD', 'LOADSTOPF', 'LOADSTOPD', 'LOADD', 'LOADR', 'LOADF', 'LOADI', 'ILOADWAIT', 'ILOADTRL', 'ILOADCLEAN', 'LOAD', 'LOADAK', 'LOADCLR', 'LOADDRY', 'LOADWC', 'LOADFRZ')
            THEN 'Loading'
            WHEN JOB_CODE_NM IN('HAUL', 'HAULCLR', 'HAULDRY', 'HAULFRZ')
            THEN 'Hauling'
            WHEN JOB_CODE_NM IN('BPMCLR', 'BPMDRY', 'BPMFRZ', 'BPMCLR', 'BPMCLRNT', 'BPMDRY', 'BPMDRYNT', 'BPMFRZ', 'BPMFRZNT')
            THEN 'Building Pallet Move'
            WHEN JOB_CODE_NM IN('PUTCLR', 'PUTDRY', 'PUTFRZ', 'PUTPRO', 'PUTSTKYD', 'zNO LABEL', 'zPUTCLR', 'zPUTFRZ', 'zPUTDRY', 'PUTCLR', 'PUTCLRNT', 'PUTDRY', 'PUTDRYNT', 'PUTFRZ', 'PUTFRZNT', 'PUTMEAT', 'PUTPRO', 'PUTSTKYD')
            THEN 'Putaway'
            WHEN JOB_CODE_NM IN('PUTPIRCLR', 'PUTPIRDRY', 'PUTPIRFRZ', 'zPUTPIRCLR', 'zPUTPIRDRY', 'zPUTPIRFRZ')
            THEN 'Putaway PIR'
            WHEN JOB_CODE_NM IN('LETCLR', 'LETCLRNT', 'LETDRY', 'LETDRYNT', 'LETFRZ', 'LETFRZNT')
            THEN 'Replenishment'
            WHEN JOB_CODE_NM IN('RCV', 'RCVALL', 'RCVCLR', 'RCVDRY', 'RCVFRZ', 'RCVID', 'RCVSTG', 'BACKHL', 'RCVCLR', 'RCVDRY', 'RCVFRZ', 'RCVCLRI', 'RCVCLRP', 'RCVDRYI', 'RCVDRYP', 'RCVFRZI', 'RCVFRZP', 'RCVALL', 'UNLOAD')
            THEN 'Receiving'
            WHEN JOB_CODE_NM IN('ADMIN', 'BUILD', 'CLERK', 'FOREMAN', 'LEAD', 'LTDUTY', 'MAINT', 'SANITATION', 'SPOT', 'TRANS', 'TRAVELICPM', 'RDC')
            THEN 'Support'
            WHEN JOB_CODE_NM IN('WILLCALL', 'SWC', 'SWCCLR', 'SWCDRY', 'SWCFRZ', 'SWCPIRDRY', 'SWCPIRFRZ')
            THEN 'Will Call'
            WHEN JOB_CODE_NM IN('BUNKER', 'SBULK', 'SBULKDRY', 'SBULKFRZ', 'SFRESH', 'SHORTS', 'SICE', 'SNAVY', 'SPROD', 'SXFER', 'WRAP', 'zICPU', 'zILETDOWN', 'zIMEAT', 'SCLR', 'SDRY', 'SFRZ', 'SDRY3', 'SFRZ3', 'SCLR3', 'SCHEM', 'SJIT', 'SBULK', 'SBULKCLR', 'SBULKDRY', 'SBULKFRZ', 'SCHEM', 'SCHEM3', 'SCHX', 'SCLR', 'SCLR3', 'SDRY', 'SDRY3', 'SEGGS', 'SFRESH', 'SFRZ', 'SFRZ3', 'SICE', 'SJIT', 'SPIRCLR', 'SPIRDRY', 'SPIRFRZ', 'SSEA', 'TRAVEL', 'WRAP')
            THEN 'Selection Shipped'
            WHEN JOB_CODE_NM IN('SCHEM', 'SJIT', 'SPIRDRY', 'TRAVEL', 'SPIRDRY', 'SPIRFRZ')
            THEN 'Selection Merged'
            WHEN JOB_CODE_NM IN('zFRUN') THEN 'Runner'
            ELSE 'Indirect'
       END AS CUSTOM_WORK_CATEGORY ,
     wc.WCT_NAME AS WORK_CATEGORY ,
     j.DESCRIPTION AS JOB_CODE_DSC ,
     j.AISLEAREA_INT_ID AS AISLE_AREA_ID ,
     j.AISLE_AREA_ID AS AISLE_AREA_NM ,
     j.DIRECT AS DIRECT_FLG ,
     j.ISBREAK AS BREAK_FLG ,
     j.ISMEASURED AS MEASURED_FLG ,
     j.ISPAID AS PAID_FLG ,
     j.REQ_APPROVAL AS REQ_APPROVAL_FLG ,
     j.MASK_LEVEL AS MASK_LEVEL ,
     j.EXT_ASSIGNMENTTIME AS EXT_ASSIGNMENT_TIME ,
     j.INT_ASSIGNMENTTIME AS INT_ASSIGNMENT_TIME ,
     j.EXT_ORDERTIME AS EXT_ORDER_TIME ,
     j.INT_ORDERTIME AS INT_ORDER_TIME ,
     j.SRC_ID
            FROM {{ source('GOLD_RED_PRAIRIE', 'JOBCODE') }} j
       LEFT JOIN {{ source('GOLD_RED_PRAIRIE', 'WORKCATEGORY') }} wc
              ON j.WCT_INT_ID = wc.WCT_INT_ID
             AND j.WH_ID = wc.WH_ID
             AND j.SRC_ID = wc.SRC_ID)
/* Outcome */
     SELECT *
       FROM cleaned_job_code
      WHERE 1=1
        AND ROW_COUNT = 1)

/* Outcome */
     SELECT *
       FROM SOURCE