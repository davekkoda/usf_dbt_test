{{ config(materialized='view') }}

WITH source -- the CTE view name
	AS(
     SELECT PAYCODEID AS PAY_CODE_ID
          , NAME AS PAY_CODE_NM
          , CASE WHEN NAME IN('DC1-Dry-Class', 'F1Z-Regular Freezer', 'F2Z-Regular Freezer', 'F3Z-Regular Freezer', 'FZ5-Regular-Freezer', 'FZD-Freezer Pay Diff .25', 'FZP-Freezer-Class', 'REG-Regular', 'RIN-Regular Incentive', 'X01-Shift .07', 'X08-Shift .20', 'X10-Shift .25', 'X16-Shift .30', 'X17-Shift .45', 'X18-Shift OT .375', 'X21-Shift .40', 'X22-Shift OT .40', 'X23-Shift .50', 'X24-Shift .75', 'X27-Shift 1.00', 'X29-Shift .22', 'X30-Shift .33', 'X32-Shift .325', 'X37-Shift .45', 'X41-Shift .55', 'X41-Shift 1.50', 'X43-Shift .55 DT', 'ZBPMCLR', 'ZBPMDRY', 'ZBPMDRYNT', 'ZBPMFRZ', 'ZBPMFRZNT', 'Z-Down Time', 'ZIBATTI', 'ZIBATTO', 'ZISTARTI', 'ZISTARTO', 'ZLETCLR', 'ZLETCLRNT', 'ZLETDRY', 'ZLETFRZ', 'ZLETFRZNT', 'Z-ON-CALL', 'ZPUTCLR', 'ZPUTDRY', 'ZPUTDRYNT', 'ZPUTFRZ', 'ZPUTPIRDRY', 'ZPUTPIRFRZ', 'ZRCVCLR', 'ZRCVDRY', 'Z-REGULAR', 'ZSCHEM', 'ZSCLR3', 'ZSDRY3', 'ZSPIRDRY', 'ZSPIRFRZ', 'Z-STD')
                 THEN 'Regular'
                 WHEN NAME IN('DC2-Dry-Class-Overtime', 'DOT-Double Time', 'F1O-Freezer Overtime', 'F2O-Freezer Overtime', 'F3O-Freezer Overtime', 'FZ6-Freezer-Overtime', 'FZE-Freezer OT Diff .375', 'FZO-Freezer-Class-Overtime', 'OIN-Overtime Incentive', 'OVT-Overtime', 'ROV-Rover-CntOVT', 'Unapproved Overtime', 'VOT- Vac Overtime', 'X05-Shf OT .07', 'X09-Sft OT .30', 'X18-Sht OT .375', 'X33-Shift OT .325', 'X36-Shift DOT .30', 'X38-Shift OT .45', 'X39-Shift DOT .45', 'X40-Shift DOT .40', 'X42-Shift .55 OT', 'Z-DOUBLETIME', 'Z-OVERTIME')
                 THEN 'Overtime'
                 WHEN NAME IN('LDP-Light Duty', 'Z-Light Duty')
                 THEN 'Light Duty'
                 WHEN NAME IN('HFL-Holiday Floater Union Only', 'HLS-Birthday', 'HLS-Hol-Shift', 'HLS-Hol-Shift-No-OT', 'HOL-Birthday', 'HOL-CntNoOvt', 'HOL-CntOVT1.5', 'HOL-CntOVT-noshif', 'HOL-Floater', 'HOL-noshif', 'HOS-Holiday-Salaried', 'HWD-Worked Holiday', 'Z-HOL', 'Z-HOL-CntOvt', 'Z-HOL-Float', 'Z-HOLIDAY')
                 THEN 'Holiday'
                 WHEN NAME IN('PER-CntOVT-Personal', 'PER-Personal', 'PIN-Personal Incentive', 'PPO-Personal Payout', 'PPS-Unused Personal', 'PRS-Personal-Salaried', 'PVC-Vacation-Current-Year-Payout', 'PWS-Personal wo shift', 'TVC-Terminated-Vacation', 'UPE-Union Personal Day', 'UPR-Union Vacation Prior Year', 'VAC-cntOVAC-noshif', 'VAC-Vac-noshif', 'VAS-Vacation-Salaried', 'VPR-Vac-Prior-Year-Payout', 'VPS-Vac-Pay-Supplemental', 'VSH-cntOVAC-Sshift', 'VSH-Vac-Shift', 'Z-OFF-DAY', 'Z-PER-CntOVT-Personal', 'Z-PER-Personal', 'Z-Union-Bus', 'Z-VAC', 'Z-VAC-cntOVAC-noshif')
                 THEN 'Vacation/PTO'
                 WHEN NAME IN('FNL-CntOVT-Funeral', 'FNL-Funeral Pa', 'FWS-Funeral wo shift', 'PER-SIK', 'PER-SIK-Unexcused', 'SCS-Sick-Salaried', 'SIK-CntOVT-Sick', 'SIK-Sick', 'SIK-Sick-Excused', 'SIK-Sick-Family', 'SKS-CntOVT-Sick-shift', 'SKS-Sick-Shift', 'SPS-Unused Sick', 'UFL-Union Funeral Leave', 'VAC-SIK', 'VAC-SIK-Unexcused', 'Z-EXCUSED', 'Z-No-Call-No-Show', 'Z-SIK', 'ZUNEXCUSED', 'Z-UNEXCUSED')
                 THEN 'Sick'
                 WHEN NAME IN('S60-ShortTermDisability60', 'Z-FMLA', 'Z-LOA', 'ZWORKCOMP', 'Z-WORKERS-COMP')
                 THEN 'Short Term'
                 ELSE 'Other'
            END AS PAY_CODE_CATEGORY
          , TYPE AS PAY_CODE_TYPE
          , EDIT_EXCUSE_ABSN AS EXCUSED_ABSENCE_FLG
          , WAGEADDITION AS WAGE_ADDITION
          , WAGEMULTIPLY AS WAGE_MULTIPLY
          , UPDATE_DTM AS CDW_UPD_TS
          , UPDATEDBYUSRACCTID AS CDW_UPD_USR_ID
        FROM {{ source('GOLD_KRONOS', 'PAYCODE') }}
    )

/* Outcome */
     SELECT *
       FROM SOURCE