USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[usp_DQ_Get_Labtest]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Hong Zhang
-- Create date: 20090128
-- Description:	This procedure collects errorneous lab test data and insert to a data validation table.	
--							It returns subsets of data based on the params that the user specifies.		
--							The procedure is a subprocedure of [usp_DQ_Main]
-- Modify Date: 20100805 (R4.0.2)
-- Modified By: Hong Zhang
-- =============================================
CREATE PROCEDURE .[dbo].[usp_DQ_Get_Labtest]
@need_error			tinyint,
@need_warning	 tinyint,
@need_ELR			 tinyint,
@min_last_chg_time	datetime=null
AS

BEGIN

	SET NOCOUNT ON
	/* lab report  data validation */
	PRINT 'Lab Test Report Validation Checks Insert Started...'+ CONVERT(varchar(20), getdate(),120) 

	IF @need_error = 1 AND @need_warning = 1
	BEGIN
		INSERT INTO DATA_VALIDATION
			(MESSAGE_CD, MESSAGE_TYPE, MESSAGE_DESC, 
			PATIENT_LOCAL_ID, PATIENT_LAST_NM, PATIENT_FIRST_NM, EVENT_LOCAL_ID, EVENT_TYPE, EVENT_LAST_CHG_TIME, 
			JURISDICTION_NM, MMWR_WEEK, MMWR_YEAR, CONDITION_NM, PROGRAM_JURISDICTION_OID,PROGRAM_AREA_NM,
			PROGRAM_AREA_CD,CONDITION_CD,JURISDICTION_CD,INVESTIGATOR_NM,INVESTIGATION_STATUS,INV_CASE_STATUS,
			ELR_IND)
		SELECT DISTINCT	 --somehow there are duplicates in the lab_test table for the following fields
						t2.data_validation_message_cd, t2.data_validation_message_type, t2.data_validation_message_desc, 
						t6.PATIENT_LOCAL_ID, t6.PATIENT_LAST_NAME, t6.PATIENT_FIRST_NAME, t5.local_id,'Lab Report', t5.last_chg_time, 
						t3.jurisdiction_nm, NULL, NULL, t5.condition_desc_txt, t5.program_jurisdiction_oid, t5.prog_area_desc_txt, 
						t5.prog_area_cd, t5.condition_cd, t3.jurisdiction_cd, NULL, NULL, NULL, 
						t3.elr_ind
		FROM LAB_TEST t3  LEFT JOIN LAB_TEST_RESULT t4 
					ON t3.lab_test_key = t4.lab_test_key
					LEFT JOIN D_PATIENT t6 
					ON t6.PATIENT_KEY = t4.patient_key
					LEFT JOIN EVENT_METRIC t5  
					ON t3.lab_rpt_local_id = t5.local_id  -- both varchar
					CROSS JOIN data_validation_message	 t2 
		WHERE (t5.last_chg_time IS NOT NULL -- added this clause per Jit 01/27/09 
							AND t5.last_chg_time >= COALESCE(@min_last_chg_time, t5.last_chg_time)
							AND t3.lab_test_type = 'Order'
							AND COALESCE(t3.record_status_cd,'') <> 'INACTIVE'	   -- would like to capture null as well
							AND ( elr_ind = CASE @need_elr WHEN 1 THEN elr_ind ELSE NULL  END
											OR COALESCE(t3.elr_ind,'') <>'Y'   
										)
							)
					AND  (
								( t2.data_validation_message_id = 8 AND (t6.PATIENT_DOB > t3.specimen_collection_dt))  -- Person DOB > Specimen Collection Date LAB100
								OR	( t2.data_validation_message_id = 9 AND (t3.lab_test_dt > t3.lab_rpt_received_by_ph_dt))  -- Lab Report Date > Date Received by Public Health LAB101
								OR	( t2.data_validation_message_id = 26 AND (t3.specimen_collection_dt IS NULL))  -- Lab Report Date > Date Received by Public Health LAB102
								OR	( t2.data_validation_message_id = 27 AND (t4.investigation_key = 1AND t3.record_status_cd = 'ACTIVE'))  -- Orphaned Lab Report (Lab without an investigation) LAB103
								) 
	END

	ELSE IF @need_error = 1 AND @need_warning = 0
	BEGIN
		INSERT INTO DATA_VALIDATION
			(MESSAGE_CD, MESSAGE_TYPE, MESSAGE_DESC, 
			PATIENT_LOCAL_ID, PATIENT_LAST_NM, PATIENT_FIRST_NM, EVENT_LOCAL_ID, EVENT_TYPE, EVENT_LAST_CHG_TIME, 
			JURISDICTION_NM, MMWR_WEEK, MMWR_YEAR, CONDITION_NM, PROGRAM_JURISDICTION_OID,PROGRAM_AREA_NM,
			PROGRAM_AREA_CD,CONDITION_CD,JURISDICTION_CD,INVESTIGATOR_NM,INVESTIGATION_STATUS,INV_CASE_STATUS,
			ELR_IND)
		SELECT	DISTINCT
						t2.data_validation_message_cd, t2.data_validation_message_type, t2.data_validation_message_desc, 
						t6.PATIENT_LOCAL_ID, t6.PATIENT_LAST_NAME, t6.PATIENT_FIRST_NAME, t5.local_id,'Lab Report', t5.last_chg_time, 
						t3.jurisdiction_nm, NULL, NULL, t5.condition_desc_txt, t5.program_jurisdiction_oid, t5.prog_area_desc_txt, 
						t5.prog_area_cd, t5.condition_cd, t3.jurisdiction_cd, NULL, NULL, NULL, 
						t3.elr_ind
		FROM LAB_TEST t3  LEFT JOIN LAB_TEST_RESULT t4  
					ON t3.lab_test_key = t4.lab_test_key
					LEFT JOIN D_PATIENT t6  
					ON t6.PATIENT_KEY = t4.patient_key
					LEFT JOIN EVENT_METRIC t5 
					ON t3.lab_rpt_local_id = t5.local_id  -- both varchar
					CROSS JOIN data_validation_message	 t2 
		WHERE ( 
							t5.last_chg_time IS NOT NULL -- added this clause to remove nulls per Jit 01/27/09 
							AND t5.last_chg_time >= COALESCE(@min_last_chg_time, t5.last_chg_time)
							AND t3.lab_test_type = 'Order'
							AND COALESCE(t3.record_status_cd,'') <> 'INACTIVE'	   -- would like to capture null as well
							AND ( elr_ind = CASE @need_elr WHEN 1 THEN elr_ind ELSE NULL  END
											OR COALESCE(t3.elr_ind,'') <>'Y'   
										)
							)
					AND  (
								( t2.data_validation_message_id = 8 AND (t6.PATIENT_DOB > t3.specimen_collection_dt))  -- Person DOB > Specimen Collection Date LAB100
								OR	( t2.data_validation_message_id = 9 AND (t3.lab_test_dt > t3.lab_rpt_received_by_ph_dt))  -- Lab Report Date > Date Received by Public Health LAB101
	--							OR	( t2.data_validation_message_id = 26 AND (t3.specimen_collection_dt IS NULL))  -- Lab Report Date > Date Received by Public Health LAB102
	--							OR	( t2.data_validation_message_id = 27 AND (t4.investigation_key = 1AND t3.record_status_cd = 'ACTIVE'))  -- Orphaned Lab Report (Lab without an investigation) LAB103
								) 
	END

	ELSE IF @need_error = 0 AND @need_warning = 1
	BEGIN
		INSERT INTO DATA_VALIDATION
			(MESSAGE_CD, MESSAGE_TYPE, MESSAGE_DESC, 
			PATIENT_LOCAL_ID, PATIENT_LAST_NM, PATIENT_FIRST_NM, EVENT_LOCAL_ID, EVENT_TYPE, EVENT_LAST_CHG_TIME, 
			JURISDICTION_NM, MMWR_WEEK, MMWR_YEAR, CONDITION_NM, PROGRAM_JURISDICTION_OID,PROGRAM_AREA_NM,
			PROGRAM_AREA_CD,CONDITION_CD,JURISDICTION_CD,INVESTIGATOR_NM,INVESTIGATION_STATUS,INV_CASE_STATUS,
			ELR_IND)
		SELECT	DISTINCT
						t2.data_validation_message_cd, t2.data_validation_message_type, t2.data_validation_message_desc, 
						t6.PATIENT_LOCAL_ID, t6.PATIENT_LAST_NAME, t6.PATIENT_FIRST_NAME, t5.local_id,'Lab Report', t5.last_chg_time, 
						t3.jurisdiction_nm, NULL, NULL, t5.condition_desc_txt, t5.program_jurisdiction_oid, t5.prog_area_desc_txt, 
						t5.prog_area_cd, t5.condition_cd, t3.jurisdiction_cd, NULL, NULL, NULL, 
						t3.elr_ind
		FROM LAB_TEST t3  LEFT JOIN LAB_TEST_RESULT t4 
					ON t3.lab_test_key = t4.lab_test_key
					LEFT JOIN D_PATIENT t6 
					ON t6.PATIENT_KEY = t4.patient_key
					LEFT JOIN EVENT_METRIC t5  
					ON t3.lab_rpt_local_id = t5.local_id  -- both varchar
					CROSS JOIN data_validation_message	 t2 
		WHERE ( 
							t5.last_chg_time IS NOT NULL -- added this clause per Jit 01/27/09 
							AND t5.last_chg_time >= COALESCE(@min_last_chg_time, t5.last_chg_time)
							AND t3.lab_test_type = 'Order'
							AND COALESCE(t3.record_status_cd,'') <> 'INACTIVE'	   -- would like to capture null as well
							AND ( elr_ind = CASE @need_elr WHEN 1 THEN elr_ind ELSE NULL  END
											OR COALESCE(t3.elr_ind,'') <>'Y'   
										)
							)
					AND  (
	--							( t2.data_validation_message_id = 8 AND (t6.person_dob > t3.specimen_collection_dt))  -- Person DOB > Specimen Collection Date LAB100
	--							OR	( t2.data_validation_message_id = 9 AND (t3.lab_test_dt > t3.lab_rpt_received_by_ph_dt))  -- Lab Report Date > Date Received by Public Health LAB101
							 			( t2.data_validation_message_id = 26 AND (t3.specimen_collection_dt IS NULL))  -- Lab Report Date > Date Received by Public Health LAB102
								OR	( t2.data_validation_message_id = 27 AND (t4.investigation_key = 1AND t3.record_status_cd = 'ACTIVE'))  -- Orphaned Lab Report (Lab without an investigation) LAB103
								) 
	END
    PRINT 'Lab Test Report Validation Checks Insert Completed...'+ CONVERT(varchar(20), getdate(),120) 

END
GO
