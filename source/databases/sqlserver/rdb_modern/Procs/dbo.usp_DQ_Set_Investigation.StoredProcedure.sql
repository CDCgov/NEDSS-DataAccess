USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[usp_DQ_Set_Investigation]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Hong Zhang
-- Create date: 20090128
-- Description:	This procedure collects all invesigation data and writes to a dq_all_case table.
--							It returns subsets of data based ON the params the user specified.
--							The procedure is a subprocedure of [usp_DQ_Main]
-- Modify Date: 20100805 (R4.0.2)
-- Modified By: Hong Zhang
--===========================================
CREATE  PROCEDURE [dbo].[usp_DQ_Set_Investigation] 
@min_last_chg_time	datetime=null
AS

BEGIN

	SET NOCOUNT ON

	/* Clean up DQ_ALL_CASE table that contains all the cases */
	TRUNCATE TABLE DQ_ALL_CASE 

	PRINT 'DQ_ALL_CASE Insert Started...' + CONVERT(varchar(20), getdate(),120) 

	/* Begin insert records into DQ_ALL_CASE table with data FROM tables FROM 7 cases */
	/* Insert records FROM CRS_CASE */
	INSERT INTO DQ_ALL_CASE (person_key, investigation_key, crs_case_count,rubella_case_count, measles_case_count, generic_case_count,bmird_case_count, hepatitis_case_count, pertussis_case_count, case_cd) 
	SELECT CRS_CASE.patient_key, CRS_CASE.investigation_key,1,0,0,0,0,0,0,'CRS' FROM CRS_CASE 
	INNER JOIN INVESTIGATION  ON CRS_CASE.investigation_key = INVESTIGATION.investigation_key and INVESTIGATION.record_status_cd!='INACTIVE' 

	/* Insert records FROM RUBELLA_CASE */
	INSERT INTO DQ_ALL_CASE (person_key, investigation_key, crs_case_count,rubella_case_count, measles_case_count, generic_case_count,bmird_case_count, hepatitis_case_count, pertussis_case_count, case_cd, pregnant_male_ind) 
	SELECT RUBELLA_CASE.patient_key, RUBELLA_CASE.investigation_key,1,0,0,0,0,0,0,'RUBELLA', pregnancy_ind FROM RUBELLA_CASE  
	INNER JOIN INVESTIGATION  ON RUBELLA_CASE.investigation_key = INVESTIGATION.investigation_key and INVESTIGATION.record_status_cd!='INACTIVE' 

	/* Insert records FROM MEASLES_CASE */
	INSERT INTO DQ_ALL_CASE (person_key, investigation_key, crs_case_count,rubella_case_count, measles_case_count, generic_case_count,bmird_case_count, hepatitis_case_count, pertussis_case_count, case_cd) 
	SELECT MEASLES_CASE.patient_key, MEASLES_CASE.investigation_key,1,0,0,0,0,0,0,'MEASLES' FROM MEASLES_CASE 
	 INNER JOIN INVESTIGATION  ON MEASLES_CASE.investigation_key = INVESTIGATION.investigation_key and INVESTIGATION.record_status_cd!='INACTIVE' 

	/*  Insert records FROM GENERIC_CASE */
	INSERT INTO DQ_ALL_CASE (person_key, investigation_key, crs_case_count,rubella_case_count, measles_case_count, generic_case_count,bmird_case_count, hepatitis_case_count, pertussis_case_count, case_cd, pregnant_male_ind) 
	SELECT GENERIC_CASE.patient_key, GENERIC_CASE.investigation_key,1,0,0,0,0,0,0,'GENERIC', GENERIC_CASE.patient_pregnancy_status FROM GENERIC_CASE  
	INNER JOIN INVESTIGATION  ON GENERIC_CASE.investigation_key = INVESTIGATION.investigation_key and INVESTIGATION.record_status_cd!='INACTIVE' 

	/*  Insert records FROM BMIRD_CASE */
	INSERT INTO DQ_ALL_CASE (person_key, investigation_key, crs_case_count,rubella_case_count, measles_case_count, generic_case_count,bmird_case_count, hepatitis_case_count, pertussis_case_count, case_cd, pregnant_male_ind) 
	SELECT BMIRD_CASE.patient_key, BMIRD_CASE.investigation_key,1,0,0,0,0,0,0,'BMIRD', pregnant_ind FROM BMIRD_CASE 
	 INNER JOIN INVESTIGATION  ON BMIRD_CASE.investigation_key = INVESTIGATION.investigation_key and INVESTIGATION.record_status_cd!='INACTIVE' 

	/*  Insert records FROM HEPATITIS_CASE */
	INSERT INTO DQ_ALL_CASE (person_key, investigation_key, crs_case_count,rubella_case_count, measles_case_count, generic_case_count,bmird_case_count, hepatitis_case_count, pertussis_case_count, case_cd, pregnant_male_ind) 
	SELECT HEPATITIS_CASE.patient_key, HEPATITIS_CASE.investigation_key,1,0,0,0,0,0,0,'HEPATITIS', HEPATITIS_CASE.patient_pregnant_ind FROM HEPATITIS_CASE 
	 INNER JOIN INVESTIGATION  ON HEPATITIS_CASE.investigation_key = INVESTIGATION.investigation_key and INVESTIGATION.record_status_cd!='INACTIVE' 

	/*  Insert records FROM PERTUSIS_CASE */
	INSERT INTO DQ_ALL_CASE (person_key, investigation_key, crs_case_count,rubella_case_count, measles_case_count, generic_case_count,bmird_case_count, hepatitis_case_count, pertussis_case_count, case_cd) 
	SELECT PERTUSSIS_CASE.patient_key, PERTUSSIS_CASE.investigation_key,1,0,0,0,0,0,0,'PERTUSSIS' FROM PERTUSSIS_CASE  
	INNER JOIN INVESTIGATION  ON PERTUSSIS_CASE.investigation_key = INVESTIGATION.investigation_key and INVESTIGATION.record_status_cd!='INACTIVE' 

	/* End insert records into DQ_ALL_CASE table with data FROM tables FROM 7 cases */

	PRINT 'DQ_ALL_CASE Insert completed...' + CONVERT(varchar(20), getdate(),120) 

	PRINT 'DQ_ALL_CASE Update Started...'+ CONVERT(varchar(20), getdate(),120) 

	/* get date from event_metric	*/
	UPDATE t1
	SET  t1.event_uid = t3.local_id, 
	t1.event_type = 'Investigation', 
	t1.last_chg_time = t3.last_chg_time, 
	t1.condition_cd = t3.condition_cd, 
	t1.condition_nm = t3.condition_desc_txt, 
	t1.security_oid = t3.program_jurisdiction_oid, 
	t1.program_area_nm = t3.prog_area_desc_txt, 
	t1.program_area_cd = t3.prog_area_cd, 
	t1.jurisdiction_cd = t3.jurisdiction_cd 
	FROM DQ_ALL_CASE t1 INNER JOIN INVESTIGATION t2
	on t1.investigation_key  =  t2.investigation_key
	 INNER JOIN event_metric t3
	on t2.inv_local_id = t3.local_id

	/* remove older records based ON date parameters */
	IF @min_last_chg_time IS NOT NULL
	BEGIN
		DELETE FROM DQ_ALL_CASE WHERE last_chg_time <  @min_last_chg_time
	END

	/* get INVESTIGATION detail */
	UPDATE t1
	SET
		t1.investigation_local_id = t2.inv_local_id , 
		t1.illness_onset_dt = t2.illness_onset_dt, 
		t1.illness_diagnosis_dt = t2.diagnosis_dt, 
		t1.earliest_rpt_to_county_dt = t2.earliest_rpt_to_cnty_dt, 
		t1.earliest_rpt_to_state_dt = t2.earliest_rpt_to_state_dt, 
		t1.investigation_start_dt = t2.inv_start_dt, 
		t1.investigation_status = t2.investigation_status, 
		t1.inv_case_status = t2.inv_case_status, 
		t1.mmwr_week = t2.case_rpt_mmwr_wk, 
		t1.mmwr_year = t2.case_rpt_mmwr_yr, 
		t1.jurisdiction_nm = t2.jurisdiction_nm, 
		t1.die_from_this_illness_ind = t2.die_frm_this_illness_ind 
	FROM DQ_ALL_CASE t1 INNER JOIN INVESTIGATION t2
	on t1.investigation_key = t2.investigation_key

	/* get PERSON info */
	UPDATE t1
	SET t1.person_local_id  =  t2.PATIENT_LOCAL_ID, 
	t1.person_birth_dt  =  t2.PATIENT_DOB, 
	t1.person_death_dt  =  t2.PATIENT_DECEASED_DATE, 
	t1.person_first_nm  =  t2.PATIENT_FIRST_NAME, 
	t1.person_last_nm  =  t2.PATIENT_LAST_NAME, 
	t1.person_curr_gender  =  CASE WHEN t2.PATIENT_CURRENT_SEX LIKE 'F%' THEN 'F' WHEN t2.PATIENT_CURRENT_SEX LIKE 'M%' THEN 'M' END, 
	t1.person_hispanic_ind  =  t2.PATIENT_ETHNICITY,
	t1.PERSON_state = T2.PATIENT_STATE,
	t1.PERSON_county=T2.PATIENT_COUNTY,
	t1.PATIENT_RACE_CALCULATED=T2.PATIENT_RACE_CALCULATED
	FROM DQ_ALL_CASE t1 INNER JOIN d_patient t2
	ON t1.person_key  =  t2.PATIENT_KEY
	/*1. COLUMN PERSON_RACE_KEY MUST BE CONVERTED TO RACE AS WE HAVE RACE NOT KEY FOR THIS INFORMATION*/


	/*  Get investigator key */	
	UPDATE t1
	SET t1.investigator_key = t2.investigator_key
	FROM DQ_ALL_CASE t1 INNER JOIN CASE_COUNT t2
	ON t1.investigation_key  =  t2.investigation_key

	/* Get investigator key and name */	
	UPDATE t1
	SET t1.investigator_nm = RTRIM(t2.PROVIDER_FIRST_NAME) + ' ' + RTRIM(t2.PROVIDER_LAST_NAME)
	FROM DQ_ALL_CASE t1 INNER JOIN D_PROVIDER t2
	ON  t1.investigator_key = t2.PROVIDER_KEY

	/*  Get Race: note that it is a 1 to Many relationship and it will pick the first row. Jit Ok'ed */
	/*PRADEEP:-ALL THIS INFORMATION IS INCLUDED IN D_PATIENT TABLE DUE TO 4.01 CHANGES 

	UPDATE t1
	SET t1.PERSON_race_key = t2.PERSON_race_key
	FROM DQ_ALL_CASE t1 INNER JOIN PERSON_RACE t2
	ON t1.person_key  =  t2.person_key
	/* Get State and County: 	note that is a 1 to Many relationship and it will pci the first row. Jit Ok'ed */
	UPDATE t1
	SET t1.PERSON_state = t3.state_fips,
	 t1.PERSON_county = t3.cnty_fips 
	FROM DQ_ALL_CASE t1 INNER JOIN PERSON_LOCATION t2
	ON t1.person_key  =  t2.person_key
	 INNER JOIN LOCATION t3
	ON t2.location_key = t3.location_key*/

	PRINT 'DQ_ALL_CASE Update Completed...'+ CONVERT(varchar(20), getdate(),120) 

END
GO
