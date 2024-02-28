USE [rdb_modern]
GO
/****** Object:  View [dbo].[Lab_Test_Report]    Script Date: 1/17/2024 8:39:44 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER OFF
GO

CREATE VIEW [dbo].[Lab_Test_Report]
AS 
SELECT TOP 100 PERCENT
lt.ROOT_ORDERED_TEST_PNTR, 
lt.PARENT_TEST_PNTR, 
lt.LAB_TEST_PNTR, 
org1.ORGANIZATION_NAME AS reporting_facility, 
org2.ORGANIZATION_NAME AS ordering_facility, 
cond.PROGRAM_AREA_CD, 
cond.PROGRAM_AREA_DESC, 
lt.JURISDICTION_CD, 
lt.JURISDICTION_NM, 
lt.LAB_RPT_CREATED_DT AS lab_rpt_dt, 
lt.LAB_RPT_RECEIVED_BY_PH_DT, 
lt.ACCESSION_NBR, 
lt.SPECIMEN_SRC, 
lt.SPECIMEN_SITE, 
lt.SPECIMEN_SITE_DESC, 
lt.SPECIMEN_COLLECTION_DT, 
lt.ROOT_ORDERED_TEST_NM AS ordered_test_nm, 
lt.LAB_TEST_CD_DESC AS lab_test_nm, 
lt.LAB_TEST_DT AS resulted_dt, 
lt.LAB_TEST_STATUS, 
lt.SPECIMEN_DESC, 
lt.LAB_TEST_TYPE, 
lt.OID AS program_jurisdiction_oid, 
lt.LAB_RPT_SHARE_IND AS shared_ind, 
lrv.NUMERIC_RESULT AS numeric_result_val, 
lrv.Result_Units,
lrv.LAB_RESULT_TXT_VAL AS text_result_val, 
lrv.REF_RANGE_FRM, 
lrv.REF_RANGE_TO, 
lrv.TEST_RESULT_VAL_CD AS coded_result_val, 
lrv.TEST_RESULT_VAL_CD_DESC AS coded_result_val_desc, 
pat.PATIENT_FIRST_NAME AS patient_first_nm, 
pat.PATIENT_MIDDLE_NAME AS patient_middle_nm, 
pat.PATIENT_LAST_NAME AS patient_last_nm, 
pat.PATIENT_NAME_SUFFIX AS patient_nm_suffix, 
prov.PROVIDER_FIRST_NAME AS provider_first_nm, 
prov.PROVIDER_MIDDLE_NAME AS provider_middle_nm, 
prov.PROVIDER_LAST_NAME AS provider_last_nm, 
prov.PROVIDER_NAME_PREFIX AS provider_nm_prefix, 
pat.PATIENT_STREET_ADDRESS_1 as STREET_ADDR_1, 
pat.PATIENT_STREET_ADDRESS_2 as STREET_ADDR_2, 
--loc.CITY_FIPS AS city_cd, 
pat.PATIENT_CITY AS city_desc, 
pat.PATIENT_STATE_CODE AS state_cd, 
pat.PATIENT_STATE AS state_desc, 
pat.PATIENT_ZIP AS zip_cd, 
--loc.ZIP_SHORT_DESC AS zip_cd_desc,
ltr.RECORD_STATUS_CD AS record_status_cd,
lt.LAB_RPT_LOCAL_ID,
pat.patient_local_id,
lt.elr_ind
FROM 
LAB_TEST lt 
INNER JOIN LAB_TEST_RESULT ltr 
ON lt.LAB_TEST_KEY = ltr.LAB_TEST_KEY 
AND ltr.PATIENT_KEY <> 1 
INNER JOIN TEST_RESULT_GROUPING trg 
ON trg.TEST_RESULT_GRP_KEY = ltr.TEST_RESULT_GRP_KEY 
left JOIN LAB_RESULT_VAL lrv 
ON trg.TEST_RESULT_GRP_KEY = lrv.TEST_RESULT_GRP_KEY 
INNER JOIN CONDITION cond 
ON cond.CONDITION_KEY = ltr.CONDITION_KEY 
INNER JOIN D_PATIENT pat 
ON pat.PATIENT_KEY = ltr.PATIENT_KEY 
INNER JOIN D_PROVIDER prov 
ON prov.PROVIDER_KEY = ltr.ORDERING_PROVIDER_KEY 
INNER JOIN D_ORGANIZATION org1 
ON org1.ORGANIZATION_KEY = ltr.REPORTING_LAB_KEY 
INNER JOIN D_ORGANIZATION org2 
ON org2.ORGANIZATION_KEY = ltr.ORDERING_ORG_KEY
ORDER BY lt.ROOT_ORDERED_TEST_PNTR, lt.PARENT_TEST_PNTR, lt.LAB_TEST_PNTR;
GO
