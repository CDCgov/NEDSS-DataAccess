USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[usp_DQ_Get_Investigation]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Hong Zhang
-- Create date: 20090128
-- Description:	This procedure writes data of a specific error or warning for investigation into data_validation table.
--							It returns subsets of data based on the params the user specified.
--							The procedure is a subprocedure of [usp_DQ_Main]
-- Modify Date: 20100805 (R4.0.2)
-- Modified By: Hong Zhang
-- =============================================
CREATE PROCEDURE [dbo].[usp_DQ_Get_Investigation]
@need_error			tinyint,
@need_warning	 tinyint
AS

BEGIN

	SET NOCOUNT ON
	PRINT 'Investigation Validation Checks Started...'+ CONVERT(varchar(20), getdate(),120) 

	/* insert the Investigation errors */
	IF @need_error = 1
	BEGIN
		INSERT INTO DATA_VALIDATION
			(MESSAGE_CD, MESSAGE_TYPE, MESSAGE_DESC, 
			PATIENT_LOCAL_ID, PATIENT_LAST_NM, PATIENT_FIRST_NM, EVENT_LOCAL_ID, EVENT_TYPE, EVENT_LAST_CHG_TIME, JURISDICTION_NM, MMWR_WEEK, MMWR_YEAR, CONDITION_NM, PROGRAM_JURISDICTION_OID,PROGRAM_AREA_NM,PROGRAM_AREA_CD,CONDITION_CD,JURISDICTION_CD,INVESTIGATOR_NM,INVESTIGATION_STATUS,INV_CASE_STATUS)
		SELECT 
			t2.data_validation_message_cd, t2.data_validation_message_type, t2.data_validation_message_desc, 
			t1.person_local_id, t1.person_last_nm, t1.person_first_nm, t1.event_uid,t1.event_type, t1.last_chg_time, t1.jurisdiction_nm, t1.mmwr_week, t1.mmwr_year, t1.condition_nm, t1.security_oid, t1.program_area_nm, t1.program_area_cd, t1.condition_cd, t1.jurisdiction_cd, t1.investigator_nm, t1.investigation_status, t1.inv_case_status
		FROM dq_all_case t1 CROSS JOIN data_validation_message	 t2
		WHERE
			( t2.data_validation_message_id = 1 AND t1.person_birth_dt > t1.illness_diagnosis_dt )        -- DOB > Diagnosis Date INV101
			OR ( t2.data_validation_message_id = 2 AND t1.person_birth_dt > t1.illness_onset_dt )		 -- DOB > Onset Date INV102
			OR ( t2.data_validation_message_id = 3 AND t1.person_birth_dt > t1.earliest_rpt_to_county_dt )		         -- DOB > Date Reported to county INV103
			OR ( t2.data_validation_message_id = 4 AND t1.person_birth_dt > t1.earliest_rpt_to_state_dt )       -- DOB > Date Reported to State INV104
			OR ( t2.data_validation_message_id = 5 AND t1.person_birth_dt > t1.investigation_start_dt )		        -- DOB > Investigation Start Date INV105
			OR ( t2.data_validation_message_id = 6 AND t1.illness_onset_dt > t1.illness_diagnosis_dt   )		     -- Diagonis Date should not be prior to Illness Onset Date INV106
			OR ( t2.data_validation_message_id = 7 AND (t1.person_curr_gender = 'M' AND  t1.pregnant_male_ind like 'Y%' ))		       -- Pregnant Male INV107
	END

	/* insert the investigation warnings */
	IF @need_warning = 1
	BEGIN
		INSERT INTO DATA_VALIDATION
			(MESSAGE_CD, MESSAGE_TYPE, MESSAGE_DESC, 
			PATIENT_LOCAL_ID, PATIENT_LAST_NM, PATIENT_FIRST_NM, EVENT_LOCAL_ID, EVENT_TYPE, EVENT_LAST_CHG_TIME, JURISDICTION_NM, MMWR_WEEK, MMWR_YEAR, CONDITION_NM, PROGRAM_JURISDICTION_OID,PROGRAM_AREA_NM,PROGRAM_AREA_CD,CONDITION_CD,JURISDICTION_CD,INVESTIGATOR_NM,INVESTIGATION_STATUS,INV_CASE_STATUS)
		SELECT 
			t2.data_validation_message_cd, t2.data_validation_message_type, t2.data_validation_message_desc, 
			t1.person_local_id, t1.person_last_nm, t1.person_first_nm, t1.event_uid,t1.event_type, t1.last_chg_time, t1.jurisdiction_nm, t1.mmwr_week, t1.mmwr_year, t1.condition_nm, t1.security_oid, t1.program_area_nm, t1.program_area_cd, t1.condition_cd, t1.jurisdiction_cd, t1.investigator_nm, t1.investigation_status, t1.inv_case_status
		FROM dq_all_case t1 CROSS JOIN data_validation_message	 t2
		WHERE
--			( t2.data_validation_message_id = 10 AND (t1.die_from_this_illness_ind = 'No' AND t1.person_death_dt is not null))              -- Patient Death Date INV108  --removed 01/28/09 per civil00017051
			( t2.data_validation_message_id = 11 AND t1.illness_onset_dt is NULL )		      -- Missing Onset Date INV109
			OR ( t2.data_validation_message_id = 12 AND t1.illness_diagnosis_dt is NULL )		           -- Missing Diagnosis Date INV110
			OR ( t2.data_validation_message_id = 13 AND t1.earliest_rpt_to_county_dt is NULL )		      -- Missing Date Reported to County INV112
			OR ( t2.data_validation_message_id = 14 AND t1.earliest_rpt_to_state_dt is NULL )		            -- Missing Date Reported to State	 INV113
			OR ( t2.data_validation_message_id = 15 AND t1.investigation_start_dt is NULL )		      -- Missing Investigation Start Date INV114
			OR ( t2.data_validation_message_id = 16 AND (t1.person_first_nm is NULL or t1.person_last_nm is NULL ))		      -- Missing Patient Name INV115
			OR ( t2.data_validation_message_id = 17 AND t1.person_birth_dt is NULL )		      -- 'Missing Patient Date of Birth INV116
			OR ( t2.data_validation_message_id = 18 AND t1.person_curr_gender is NULL  )		      -- Missing Patient Sex INV117
			OR ( t2.data_validation_message_id = 19 AND t1.person_state is NULL )		      -- Missing Patient State INV118
			OR ( t2.data_validation_message_id = 20 AND (t1.PATIENT_RACE_CALCULATED is null or t1.PATIENT_RACE_CALCULATED = 'Unknown') )		      -- 'Missing Patient Race INV119
			OR ( t2.data_validation_message_id = 21 AND t1.person_hispanic_ind is null  )		      -- Missing Patient Ethnicity INV120
			OR ( t2.data_validation_message_id = 22 AND (t1.investigation_start_dt < getdate()- 30 and t1.investigation_status = 'Open') )		      -- 	-- Investigation is open for greater than 30 days INV121
			OR ( t2.data_validation_message_id = 24 AND t1.inv_case_status is NULL )		      -- Missing Case Status INV123
			OR ( t2.data_validation_message_id = 25 AND t1.person_county is NULL  )		      -- Missing Patient County INV124
			OR  /* insert the investigation duplicates warning */
	 			(t2.data_validation_message_id = 23 AND	   --Possible Duplicate Investigation (Same person LOCAL Ids having more than 1 record for the same condition) INV122
								((t1.person_local_id + '|' + t1. condition_cd) IN (SELECT  person_local_id + '|' + condition_cd
																						FROM 	(select   person_local_id,  condition_cd,   count(*) inv_dupes
																										from     dq_all_case
																										where    person_local_id is not null
																										group by person_local_id,  condition_cd
																										having   count(*) > 1)  t3
																							) 
								)
					)  
	END

	PRINT 'Investigation Validation Checks Ended...'+ CONVERT(varchar(20), getdate(),120) 

END
GO
