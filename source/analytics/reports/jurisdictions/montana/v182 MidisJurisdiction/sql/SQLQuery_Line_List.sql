DECLARE @date_value date = {{DATE_VALUE}};

-- #todo (Upasana): Update commented column selection when new data about the report is available. 
SELECT 
    P.PATIENT_MPR_UID AS PatientId,
    I.INV_CASE_STATUS AS InvestigationStatus, 
    -- INVCTE.CC_CLOSED_DT AS 'Lost to Follow-up',
    I.CASE_RPT_MMWR_YR AS 'MMWR Year',
    I.CASE_RPT_MMWR_WK AS 'MMWR Week',
    I.JURISDICTION_NM AS Jurisdiction,
    C.CONDITION_DESC AS Condition,
    P.PATIENT_LAST_NAME AS 'Patient Last Name',
    P.PATIENT_FIRST_NAME AS 'Patient First Name',
    P.PATIENT_DOB AS 'Patient DOB',
    P.PATIENT_STREET_ADDRESS_1 AS 'Street Address',
    P.PATIENT_CITY AS City,
    P.PATIENT_ZIP AS 'Zip Code',
    P.PATIENT_CURRENT_SEX AS 'Current Sex',
    P.PATIENT_RACE_CALCULATED AS Race, 
    P.PATIENT_ETHNICITY AS Ethnicity,
    I.ILLNESS_ONSET_DT AS 'Illness Onset Date',
    I.HSPTLIZD_IND AS 'Hospitalized indicator',
    --'Control Measures Implemented'.,
    I.DIAGNOSIS_DT AS 'Diagnosis Date',
    I.EARLIEST_RPT_TO_CNTY_DT AS 'Report to County Date',
    I.EARLIEST_RPT_TO_STATE_DT AS 'State Notification Date',
    I.EARLIEST_RPT_TO_PHD_DT AS 'Diagnosis to LHJ',
    DATEDIFF(DAY, I.EARLIEST_RPT_TO_PHD_DT, I.EARLIEST_RPT_TO_STATE_DT) AS 'Days LHJ to State',
    I.PROGRAM_AREA_DESCRIPTION AS 'Program',
    -- INVCTE.CA_PATIENT_INTV_STATUS AS 'Interviewed?',
    DIH.HIV_900_RESULT AS 'HIV Status',
    DIH.HIV_REFER_FOR_900_TEST AS 'HIV Test Referred?',
    DIH.HIV_LAST_900_TEST_DT AS 'HIV Tested?',
    DIRF.RSK_SEX_W_KNWN_MSM_12M_FML_IND AS 'MSM Asked?', --Set where clause to "D-did not ask but we are looking at" to null
    DIT.TRT_TREATMENT_START_DT AS 'Earliest Treatment',
    DATEDIFF(DAY, I.DIAGNOSIS_DT , DIT.TRT_TREATMENT_START_DT) AS 'Diag To Treatment', 
    I.PATIENT_PREGNANT_IND AS 'Pregnant?',
    --'App Treated? (MIDIS)',
    --'App Treated? (Calc)',
    P.PATIENT_AGE_REPORTED AS 'Age in Years',
    -- INVCTE.TREATMENT_NM AS 'Treatments (App = appropriate for diag if not preg)',
    I. CASE_UID AS 'InvestigationId',
    I.INVESTIGATION_KEY AS 'InvestigationKey',
    I.CASE_TYPE AS 'Case Category',
    FPC.INVESTIGATOR_KEY AS 'InvestigatorKey',
    I.INVESTIGATION_ADDED_BY AS 'Investigator Name',
    I.INV_COMMENTS AS 'Investigation Comments'
FROM
    F_PAGE_CASE FPC
    JOIN D_PATIENT P ON P.PATIENT_KEY = FPC.PATIENT_KEY
    JOIN INVESTIGATION I ON I.INVESTIGATION_KEY = FPC.INVESTIGATOR_KEY
    JOIN CONDITION C ON C.CONDITION_KEY = FPC.CONDITION_KEY
    JOIN D_INV_HIV DIH ON DIH.D_INV_HIV_KEY = FPC.D_INV_HIV_KEY
    JOIN D_INV_RISK_FACTOR DIRF ON DIRF.D_INV_RISK_FACTOR_KEY = FPC.D_INV_RISK_FACTOR_KEY 
    JOIN D_INV_TREATMENT DIT ON DIT.D_INV_TREATMENT_KEY = FPC.D_INV_TREATMENT_KEY
WHERE 
    I.CASE_RPT_MMWR_YR = YEAR(@date_value) 
    AND I.CASE_RPT_MMWR_WK = DATEPART(WK, @date_value)