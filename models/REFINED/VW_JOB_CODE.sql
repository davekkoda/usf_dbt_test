{{ config(materialized='view') }}

WITH source -- the CTE view name
	AS(

WITH
     cleaned_job_code AS (SELECT row_number() OVER (partition by j.jobcodeintid, j.jobcodeid, j.description
                                                   ORDER BY j.src_id) row_count,
     j.jobcodeintid AS job_code_id ,
     j.jobcodeid AS job_code_nm ,
     j.wh_id
     , CASE WHEN JOB_CODE_NM IN('LOADSTARTF', 'LOADSTARTD', 'LOADSTOPF', 'LOADSTOPD', 'LOADD', 'LOADR', 'LOADF', 'LOADI', 'ILOADWAIT', 'ILOADTRL', 'ILOADCLEAN', 'LOAD', 'LOADAK', 'LOADCLR', 'LOADDRY', 'LOADWC', 'LOADFRZ')
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
     wc.wct_name AS work_category ,
     j.description AS job_code_dsc ,
     j.aislearea_int_id AS aisle_area_id ,
     j.aisle_area_id AS aisle_area_nm ,
     j.direct AS direct_flg ,
     j.isbreak AS break_flg ,
     j.ismeasured AS measured_flg ,
     j.ispaid AS paid_flg ,
     j.req_approval AS req_approval_flg ,
     j.mask_level AS mask_level ,
     j.ext_assignmenttime AS ext_assignment_time ,
     j.int_assignmenttime AS int_assignment_time ,
     j.ext_ordertime AS ext_order_time ,
     j.int_ordertime AS int_order_time ,
     j.src_id
       FROM {{ source('GOLD_RED_PRAIRIE', 'JOBCODE') }} j
       LEFT JOIN {{ source('GOLD_RED_PRAIRIE', 'WORKCATEGORY') }} wc
              ON j.wct_int_id = wc.wct_int_id
            AND j.wh_id = wc.wh_id
            AND j.src_id = wc.src_id)
/* Outcome */
     SELECT  *
       FROM cleaned_job_code
      WHERE 1=1
        AND ROW_COUNT = 1
    )

SELECT * FROM source -- from the CTE view build a new reference with this filename
