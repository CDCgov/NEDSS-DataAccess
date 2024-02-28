USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[sp_F_PAGE_CASE]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE  PROCEDURE [dbo].[sp_F_PAGE_CASE]
  @batch_id BIGINT
 as

  BEGIN
    DECLARE @RowCount_no INT ;
    DECLARE @Proc_Step_no FLOAT = 0 ;
    DECLARE @Proc_Step_Name VARCHAR(200) = '' ;
	DECLARE @batch_start_time datetime2(7) = null ;
	DECLARE @batch_end_time datetime2(7) = null ;
  
   BEGIN TRY
    
	SET @Proc_Step_no = 1;
	SET @Proc_Step_Name = 'SP_Start';

	
	BEGIN TRANSACTION;
	
    INSERT INTO [dbo].[job_flow_log] (
	        batch_id
		   ,[Dataflow_Name]
		   ,[package_Name]
		    ,[Status_Type] 
           ,[step_number]
           ,[step_name]
           ,[row_count]
           )
		   VALUES
           (
		   @batch_id
           ,'F_PAGE_CASE'
           ,'F_PAGE_CASE'
		   ,'START'
		   ,@Proc_Step_no
		   ,@Proc_Step_Name
           ,0
		   );
  
    COMMIT TRANSACTION;
	
	
	select @batch_start_time = batch_start_dttm,@batch_end_time = batch_end_dttm
	from [dbo].[job_batch_log]
	 where status_type = 'start' and type_code='MasterETL'
     ;

    /** TODO: (Upasana) Commented for Change Data Capture- Start: Processing logic **/	
	SELECT cdc_topic_updated_datetime, case_management_uid, cdc_id 
	INTO #TMP_CDC_Case_management
	FROM nbs_changedata.dbo.case_management
	WHERE cdc_status <> 1 AND cdc_topic_updated_datetime<=@batch_end_time;

	SELECT cdc_topic_updated_datetime, public_health_case_uid, cdc_id 
	INTO #TMP_CDC_Public_health_case
	FROM nbs_changedata.dbo.Public_health_case
	WHERE cdc_status <> 1 AND cdc_topic_updated_datetime<=@batch_end_time;

	SELECT cdc_topic_updated_datetime, nbs_act_entity_uid, cdc_id 
	INTO #TMP_CDC_NBS_act_entity
	FROM nbs_changedata.dbo.NBS_act_entity
	WHERE cdc_status <> 1 AND cdc_topic_updated_datetime<=@batch_end_time;

	BEGIN TRANSACTION;


		SET @Proc_Step_no = 2;
		SET @Proc_Step_Name = ' Generating PHC_UIDS_ALL'; 



			IF OBJECT_ID('dbo.PHC_UIDS_ALL', 'U') IS NOT NULL  
				drop table dbo.PHC_UIDS_ALL
				;


			SELECT 
				PUBLIC_HEALTH_CASE.PUBLIC_HEALTH_CASE_UID  'PAGE_CASE_UID', /* VS LENGTH =8 AS PAGE_CASE_UID 'PAGE_CASE_UID',*/
				CASE_MANAGEMENT.CASE_MANAGEMENT_UID, 
				INVESTIGATION_FORM_CD, 
				CD, 
				LAST_CHG_TIME 
			INTO  dbo.PHC_UIDS_ALL 
			FROM 
				nbs_changedata.dbo.PUBLIC_HEALTH_CASE PUBLIC_HEALTH_CASE  
			LEFT OUTER JOIN nbs_changedata.dbo.CASE_MANAGEMENT CASE_MANAGEMENT ON	PUBLIC_HEALTH_CASE.PUBLIC_HEALTH_CASE_UID= CASE_MANAGEMENT.PUBLIC_HEALTH_CASE_UID
			LEFT OUTER JOIN NBS_SRTE.dbo.CONDITION_CODE ON 	CONDITION_CODE.CONDITION_CD= PUBLIC_HEALTH_CASE.CD AND	INVESTIGATION_FORM_CD 
			NOT IN 	( 'bo.','INV_FORM_BMDGBS','INV_FORM_BMDGEN','INV_FORM_BMDNM','INV_FORM_BMDSP','INV_FORM_GEN','INV_FORM_HEPA','INV_FORM_HEPBV','INV_FORM_HEPCV','INV_FORM_HEPGEN','INV_FORM_MEA','INV_FORM_PER','INV_FORM_RUB','INV_FORM_RVCT','INV_FORM_VAR')
			where (PUBLIC_HEALTH_CASE.PUBLIC_HEALTH_CASE_UID in (Select PUBLIC_HEALTH_CASE_UID from #TMP_CDC_Public_health_case) OR
			  						  CASE_MANAGEMENT.case_management_uid in (select CASE_MANAGEMENT_UID from #TMP_CDC_Case_management)
								)

			;

		     SELECT @RowCount_no = @@ROWCOUNT;

		     INSERT INTO [dbo].[job_flow_log] 
				(batch_id,[Dataflow_Name],[package_Name] ,[Status_Type],[step_number],[step_name],[row_count])
				VALUES(@batch_id,'F_PAGE_CASE','F_PAGE_CASE','START',@Proc_Step_no,@Proc_Step_Name,@RowCount_no);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @Proc_Step_no = 3;
			SET @Proc_Step_Name = ' Generating PHC_CASE_UIDS_ALL'; 

			IF OBJECT_ID('dbo.PHC_CASE_UIDS_ALL', 'U') IS NOT NULL  
				 drop table dbo.PHC_CASE_UIDS_ALL
				 ;


				SELECT 
					PUBLIC_HEALTH_CASE.PUBLIC_HEALTH_CASE_UID  'PAGE_CASE_UID', /* VS LENGTH =8 AS PAGE_CASE_UID 'PAGE_CASE_UID',*/
					CASE_MANAGEMENT.CASE_MANAGEMENT_UID, 
					INVESTIGATION_FORM_CD, 
					CD, 
					LAST_CHG_TIME 
				 INTO  dbo.PHC_CASE_UIDS_ALL 
				FROM 
					nbs_changedata.dbo.PUBLIC_HEALTH_CASE PUBLIC_HEALTH_CASE 
				LEFT OUTER JOIN nbs_changedata.dbo.CASE_MANAGEMENT CASE_MANAGEMENT ON	PUBLIC_HEALTH_CASE.PUBLIC_HEALTH_CASE_UID= CASE_MANAGEMENT.PUBLIC_HEALTH_CASE_UID
				LEFT OUTER JOIN NBS_SRTE.dbo.CONDITION_CODE ON 	CONDITION_CODE.CONDITION_CD= PUBLIC_HEALTH_CASE.CD AND	INVESTIGATION_FORM_CD 
				NOT IN 	( 'bo.','INV_FORM_BMDGBS','INV_FORM_BMDGEN','INV_FORM_BMDNM','INV_FORM_BMDSP','INV_FORM_GEN','INV_FORM_HEPA','INV_FORM_HEPBV','INV_FORM_HEPCV','INV_FORM_HEPGEN','INV_FORM_MEA','INV_FORM_PER','INV_FORM_RUB','INV_FORM_RVCT','INV_FORM_VAR')
				where CASE_MANAGEMENT.CASE_MANAGEMENT_UID is null
				AND (PUBLIC_HEALTH_CASE.PUBLIC_HEALTH_CASE_UID in (Select PUBLIC_HEALTH_CASE_UID from #TMP_CDC_Public_health_case) OR
			  						  CASE_MANAGEMENT.case_management_uid in (select CASE_MANAGEMENT_UID from #TMP_CDC_Case_management)
								)
				;


		/*
		--????
		--CREATE TABLE 	PHC_UIDS AS
		 SELECT 
			PAGE_CASE_UID,
			INVESTIGATION_FORM_CD,
			CD,
			LAST_CHG_TIME
		FROM 
			dbo.PHC_UIDS;

		--QUIT;
		--PROC SQL;

		--CREATE TABLE 	PHC_CASE_UIDS AS 
		SELECT 
			PAGE_CASE_UID,
			INVESTIGATION_FORM_CD,
			CD,
			LAST_CHG_TIME
		FROM 
			dbo.PHC_UIDS
		WHERE 
			CASE_MANAGEMENT_UID IS NULL;


		--QUIT;
		*/

		/*

		%include etlpgm (Page_Case_Staging.sas);
		%include etlpgm (Page_Case_Lookup.sas);
		%include etlpgm (Page_Case_Dimensional.sas);
		%include etlpgm (Page_Case_Entity.sas);
		*/


		     SELECT @RowCount_no = @@ROWCOUNT;

		     INSERT INTO [dbo].[job_flow_log] 
				(batch_id,[Dataflow_Name],[package_Name] ,[Status_Type],[step_number],[step_name],[row_count])
				VALUES(@batch_id,'F_PAGE_CASE','F_PAGE_CASE','START',@Proc_Step_no,@Proc_Step_Name,@RowCount_no);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @Proc_Step_no = 4;
			SET @Proc_Step_Name = ' Generating ENTITY_KEYSTORE_INC'; 

			IF OBJECT_ID('dbo.ENTITY_KEYSTORE_INC', 'U') IS NOT NULL  
				 drop table dbo.ENTITY_KEYSTORE_INC
				 ;

		-- drop table dbo.F_S_INV_CASE
		
		IF OBJECT_ID('dbo.F_S_INV_CASE', 'U') IS NOT NULL  
			drop table dbo.F_S_INV_CASE;
				 
		-- Populate dbo.F_S_INV_CASE
		
		SELECT distinct act_uid AS PAGE_CASE_UID,last_chg_time, add_time, 
		       MAX(CASE WHEN type_cd = 'InvestgrOfPHC' THEN entity_uid END) INVESTIGATOR_uid,
		       MAX(CASE WHEN type_cd = 'PerAsReporterOfPHC' THEN entity_uid END) PERSON_AS_REPORTER_UID,
		       MAX(CASE WHEN type_cd = 'SubjOfPHC' THEN entity_uid END) patient_uid,
		       MAX(CASE WHEN type_cd = 'PhysicianOfPHC' THEN entity_uid END) PHYSICIAN_UID,
		       MAX(CASE WHEN type_cd = 'HospOfADT' THEN entity_uid END) HOSPITAL_UID,
		       MAX(CASE WHEN type_cd = 'OrgAsReporterOfPHC' THEN entity_uid END) ORG_AS_REPORTER_UID,
		       MAX(CASE WHEN type_cd = 'OrgAsClinicOfPHC' THEN entity_uid END) ORDERING_FACILTY_UID
			   INTO dbo.F_S_INV_CASE
		       FROM nbs_changedata.dbo.NBS_ACT_ENTITY NAE
		       WHERE ACT_UID IN (
				SELECT PUBLIC_HEALTH_CASE.PUBLIC_HEALTH_CASE_UID FROM nbs_changedata.dbo.PUBLIC_HEALTH_CASE PUBLIC_HEALTH_CASE  
				WHERE (PUBLIC_HEALTH_CASE.PUBLIC_HEALTH_CASE_UID 
				IN (SELECT PAGE_CASE_UID FROM dbo.PHC_UIDS WHERE CASE_MANAGEMENT_UID IS NULL)
				OR PUBLIC_HEALTH_CASE.PUBLIC_HEALTH_CASE_UID IN (Select PUBLIC_HEALTH_CASE_UID from #TMP_CDC_Public_health_case)) 
			) AND (NAE.nbs_act_entity_uid in (select nbs_act_entity_uid from #TMP_CDC_NBS_act_entity))
 		       GROUP BY act_uid,last_chg_time,add_time

			   
		--CREATE TABLE 	ENTITY_KEYSTORE AS
		SELECT 
			FSIV.ADD_TIME, 
			FSIV.LAST_CHG_TIME, 
			FSIV.PATIENT_UID, 
			COALESCE(PATIENT.PATIENT_KEY, 1)  AS PATIENT_KEY, 
			FSIV.PAGE_CASE_UID, 
			FSIV.HOSPITAL_UID, 
			COALESCE(HOSPITAL.ORGANIZATION_KEY, 1)  AS HOSPITAL_KEY, 
			FSIV.ORG_AS_REPORTER_UID, 
			COALESCE(REPORTERORG.ORGANIZATION_KEY, 1)  AS ORG_AS_REPORTER_KEY, 
			FSIV.PERSON_AS_REPORTER_UID, 
			COALESCE(PERSONREPORTER.PROVIDER_KEY, 1)  AS PERSON_AS_REPORTER_KEY, 
			FSIV.PHYSICIAN_UID, 
			COALESCE(PHYSICIAN.PROVIDER_KEY, 1)  AS PHYSICIAN_KEY, 
			FSIV.INVESTIGATOR_UID, 
			COALESCE(PROVIDER.PROVIDER_KEY, 1)  AS INVESTIGATOR_KEY, 
			COALESCE(INVESTIGATION.INVESTIGATION_KEY,1 ) AS INVESTIGATION_KEY, 
			COALESCE(CONDITION.CONDITION_KEY,1)  AS CONDITION_KEY, 
			COALESCE(LOC.GEOCODING_LOCATION_KEY, 1) AS GEOCODING_LOCATION_KEY
			--'' as TEMP

		into dbo.ENTITY_KEYSTORE_INC
		FROM dbo.F_S_INV_CASE  FSIV
			LEFT OUTER JOIN dbo.D_PATIENT PATIENT	 ON	FSIV.PATIENT_UID= PATIENT.PATIENT_UID
			LEFT OUTER JOIN dbo.D_ORGANIZATION  HOSPITAL ON 	FSIV.HOSPITAL_UID= HOSPITAL.ORGANIZATION_UID
			LEFT OUTER JOIN dbo.D_ORGANIZATION REPORTERORG ON 	FSIV.ORG_AS_REPORTER_UID= REPORTERORG.ORGANIZATION_UID
			LEFT OUTER JOIN dbo.D_PROVIDER PERSONREPORTER ON  	FSIV.PERSON_AS_REPORTER_UID= PERSONREPORTER.PROVIDER_UID
			LEFT OUTER JOIN dbo.D_PROVIDER PROVIDER ON 	FSIV.INVESTIGATOR_UID= PROVIDER.PROVIDER_UID
			LEFT OUTER JOIN dbo.D_PROVIDER PHYSICIAN ON 	FSIV.PHYSICIAN_UID= PHYSICIAN.PROVIDER_UID
			LEFT OUTER JOIN dbo.INVESTIGATION  INVESTIGATION ON 	FSIV.PAGE_CASE_UID= INVESTIGATION.CASE_UID
			LEFT OUTER JOIN dbo.PHC_CASE_UIDS_ALL  CASE_UID ON 	FSIV.PAGE_CASE_UID= CASE_UID.PAGE_CASE_UID
			LEFT OUTER JOIN dbo.CONDITION CONDITION ON 	CASE_UID.CD= CONDITION.CONDITION_CD
			LEFT JOIN dbo.GEOCODING_LOCATION AS LOC ON LOC.ENTITY_UID = PATIENT.PATIENT_UID
			;

		/*
		--QUIT;
		DATA ENTITY_KEYSTORE;
		SET ENTITY_KEYSTORE;
			IF HOSPITAL_KEY =. THEN HOSPITAL_KEY=1;
			IF ORG_AS_REPORTER_KEY =. THEN ORG_AS_REPORTER_KEY=1;
			IF PERSON_AS_REPORTER_KEY =. THEN PERSON_AS_REPORTER_KEY=1;
			IF PHYSICIAN_KEY =. THEN PHYSICIAN_KEY=1;
			IF INVESTIGATOR_KEY =. THEN INVESTIGATOR_KEY=1;
			IF F_PAGE_CASE_KEY  =. THEN F_PAGE_CASE_KEY=1;
			IF INV_CLINICAL_KEY  =. THEN INV_CLINICAL_KEY=1;
			IF INV_COMPLICATION_KEY =. THEN INV_COMPLICATION_KEY=1;
			IF INV_CONTACT_KEY =. THEN INV_CONTACT_KEY=1;
			IF INV_DEATH_KEY =. THEN INV_DEATH_KEY=1;
			IF INV_EPIDEMIOLOGY_KEY =. THEN INV_EPIDEMIOLOGY_KEY=1;
			IF INV_HIV_KEY =. THEN INV_HIV_KEY=1;
			IF INV_PATIENT_OBS_KEY =. THEN INV_PATIENT_OBS_KEY=1;
			IF INV_ISOLATE_TRACKING_KEY =. THEN INV_ISOLATE_TRACKING_KEY=1;
			IF INV_LAB_FINDING_KEY =. THEN INV_LAB_FINDING_KEY=1;
			IF INV_MEDICAL_HISTORY_KEY =. THEN INV_MEDICAL_HISTORY_KEY=1;
			IF INV_MOTHER_KEY =. THEN INV_MOTHER_KEY=1;
			IF INV_OTHER_KEY =. THEN INV_OTHER_KEY=1;
			IF INV_PREGNANCY_BIRTH_KEY =. THEN INV_PREGNANCY_BIRTH_KEY=1;
			IF INV_RESIDENCY_KEY =. THEN INV_RESIDENCY_KEY=1;
			IF INV_RISK_FACTOR_KEY =. THEN INV_RISK_FACTOR_KEY=1;
			IF INV_SOCIAL_HISTORY_KEY =. THEN INV_SOCIAL_HISTORY_KEY=1;
			IF INV_SYMPTOM_KEY =. THEN INV_SYMPTOM_KEY=1;
			IF INV_TREATMENT_KEY =. THEN INV_TREATMENT_KEY=1;
			IF INV_TRAVEL_KEY =. THEN INV_TRAVEL_KEY=1;
			IF INV_UNDER_CONDITION_KEY =. THEN INV_UNDER_CONDITION_KEY=1;
			IF INV_VACCINATION_KEY =. THEN INV_VACCINATION_KEY=1;

		RUN;
		 PROC SORT DATA=ENTITY_KEYSTORE NODUPKEY; BY PAGE_CASE_UID; RUN;
		PROC SQL;
		*/

		/*
		create table L_INV_ADMINISTRATIVE as select L_INV_ADMINISTRATIVE.PAGE_CASE_UID , L_INV_ADMINISTRATIVE.D_F_PAGE_CASE_KEY FROM dbo.L_INV_ADMINISTRATIVE ORDER BY PAGE_CASE_UID;
		create table L_INV_CLINICAL as select L_INV_CLINICAL.PAGE_CASE_UID , L_INV_CLINICAL.D_INV_CLINICAL_KEY FROM dbo.L_INV_CLINICAL ORDER BY PAGE_CASE_UID;
		create table L_INV_COMPLICATION as select L_INV_COMPLICATION.PAGE_CASE_UID , L_INV_COMPLICATION.D_INV_COMPLICATION_KEY FROM dbo.L_INV_COMPLICATION ORDER BY PAGE_CASE_UID;
		create table L_INV_CONTACT as select L_INV_CONTACT.PAGE_CASE_UID , L_INV_CONTACT.D_INV_CONTACT_KEY FROM dbo.L_INV_CONTACT ORDER BY PAGE_CASE_UID;
		create table L_INV_DEATH as select L_INV_DEATH.PAGE_CASE_UID , L_INV_DEATH.D_INV_DEATH_KEY FROM dbo.L_INV_DEATH ORDER BY PAGE_CASE_UID;
		create table L_INV_EPIDEMIOLOGY as select L_INV_EPIDEMIOLOGY.PAGE_CASE_UID , L_INV_EPIDEMIOLOGY.D_INV_EPIDEMIOLOGY_KEY FROM dbo.L_INV_EPIDEMIOLOGY ORDER BY PAGE_CASE_UID;
		create table L_INV_HIV as select L_INV_HIV.PAGE_CASE_UID , L_INV_HIV.D_INV_HIV_KEY FROM dbo.L_INV_HIV ORDER BY PAGE_CASE_UID;
		create table L_INV_PATIENT_OBS as select L_INV_PATIENT_OBS.PAGE_CASE_UID , L_INV_PATIENT_OBS.D_INV_PATIENT_OBS_KEY FROM dbo.L_INV_PATIENT_OBS ORDER BY PAGE_CASE_UID;
		create table L_INV_ISOLATE_TRACKING as select L_INV_ISOLATE_TRACKING.PAGE_CASE_UID , L_INV_ISOLATE_TRACKING.D_INV_ISOLATE_TRACKING_KEY FROM dbo.L_INV_ISOLATE_TRACKING ORDER BY PAGE_CASE_UID;
		create table L_INV_LAB_FINDING as select L_INV_LAB_FINDING.PAGE_CASE_UID , L_INV_LAB_FINDING.D_INV_LAB_FINDING_KEY FROM dbo.L_INV_LAB_FINDING ORDER BY PAGE_CASE_UID;
		create table L_INV_MEDICAL_HISTORY as select L_INV_MEDICAL_HISTORY.PAGE_CASE_UID , L_INV_MEDICAL_HISTORY.D_INV_MEDICAL_HISTORY_KEY FROM dbo.L_INV_MEDICAL_HISTORY ORDER BY PAGE_CASE_UID;
		create table L_INV_MOTHER as select L_INV_MOTHER.PAGE_CASE_UID , L_INV_MOTHER.D_INV_MOTHER_KEY FROM dbo.L_INV_MOTHER ORDER BY PAGE_CASE_UID;
		create table L_INV_OTHER as select L_INV_OTHER.PAGE_CASE_UID , L_INV_OTHER.D_INV_OTHER_KEY FROM dbo.L_INV_OTHER ORDER BY PAGE_CASE_UID;
		create table L_INV_PREGNANCY_BIRTH as select L_INV_PREGNANCY_BIRTH.PAGE_CASE_UID , L_INV_PREGNANCY_BIRTH.D_INV_PREGNANCY_BIRTH_KEY FROM dbo.L_INV_PREGNANCY_BIRTH ORDER BY PAGE_CASE_UID;
		create table L_INV_RESIDENCY as select L_INV_RESIDENCY.PAGE_CASE_UID , L_INV_RESIDENCY.D_INV_RESIDENCY_KEY FROM dbo.L_INV_RESIDENCY ORDER BY PAGE_CASE_UID;
		create table L_INV_RISK_FACTOR as select L_INV_RISK_FACTOR.PAGE_CASE_UID , L_INV_RISK_FACTOR.D_INV_RISK_FACTOR_KEY FROM dbo.L_INV_RISK_FACTOR ORDER BY PAGE_CASE_UID;
		create table L_INV_SOCIAL_HISTORY as select L_INV_SOCIAL_HISTORY.PAGE_CASE_UID , L_INV_SOCIAL_HISTORY.D_INV_SOCIAL_HISTORY_KEY FROM dbo.L_INV_SOCIAL_HISTORY ORDER BY PAGE_CASE_UID;
		create table L_INV_SYMPTOM as select L_INV_SYMPTOM.PAGE_CASE_UID , L_INV_SYMPTOM.D_INV_SYMPTOM_KEY FROM dbo.L_INV_SYMPTOM ORDER BY PAGE_CASE_UID;
		create table L_INV_TREATMENT as select L_INV_TREATMENT.PAGE_CASE_UID , L_INV_TREATMENT.D_INV_TREATMENT_KEY FROM dbo.L_INV_TREATMENT ORDER BY PAGE_CASE_UID;
		create table L_INV_TRAVEL as select L_INV_TRAVEL.PAGE_CASE_UID , L_INV_TRAVEL.D_INV_TRAVEL_KEY FROM dbo.L_INV_TRAVEL ORDER BY PAGE_CASE_UID;
		create table L_INV_UNDER_CONDITION as select L_INV_UNDER_CONDITION.PAGE_CASE_UID , L_INV_UNDER_CONDITION.D_INV_UNDER_CONDITION_KEY FROM dbo.L_INV_UNDER_CONDITION ORDER BY PAGE_CASE_UID;
		create table L_INV_VACCINATION as select L_INV_VACCINATION.PAGE_CASE_UID , L_INV_VACCINATION.D_INV_VACCINATION_KEY FROM dbo.L_INV_VACCINATION ORDER BY PAGE_CASE_UID;
		*/


		/* ????
		 %include etlpgm (Repeated_Question_dimension.sas);
		????? %include etlpgm (Repeated_Place_Dimension.sas);
		*/

		/*
		Data RDBDATA.DIMENSIONAL_KEYS; 
		MERGE 
		L_F_PAGE_CASE
		L_INV_CLINICAL
		L_INV_COMPLICATION
		L_INV_CONTACT
		L_INV_DEATH
		L_INV_EPIDEMIOLOGY
		L_INV_HIV
		L_INV_PATIENT_OBS
		L_INV_ISOLATE_TRACKING
		L_INV_LAB_FINDING
		L_INV_MEDICAL_HISTORY
		L_INV_MOTHER
		L_INV_OTHER
		L_INV_PREGNANCY_BIRTH
		L_INV_RESIDENCY
		L_INV_RISK_FACTOR
		L_INV_SOCIAL_HISTORY
		L_INV_SYMPTOM
		L_INV_TREATMENT
		L_INV_TRAVEL
		L_INV_UNDER_CONDITION
		L_INV_VACCINATION
		l_F_PAGE_CASE
		????? L_INV_PLACE_REPEAT;
		BY 
		 PAGE_CASE_UID;
		RUN;
		*/


		 SELECT @RowCount_no = @@ROWCOUNT;

		     INSERT INTO [dbo].[job_flow_log] 
				(batch_id,[Dataflow_Name],[package_Name] ,[Status_Type],[step_number],[step_name],[row_count])
				VALUES(@batch_id,'F_PAGE_CASE','F_PAGE_CASE','START',@Proc_Step_no,@Proc_Step_Name,@RowCount_no);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @Proc_Step_no = 5;
			SET @Proc_Step_Name = ' Generating DIMENSION_KEYS_PAGECASEID'; 

			IF OBJECT_ID('dbo.DIMENSION_KEYS_PAGECASEID', 'U') IS NOT NULL  
				 drop table dbo.DIMENSION_KEYS_PAGECASEID
				 ;

		 select L_INV_ADMINISTRATIVE_INC.PAGE_CASE_UID as PAGE_CASE_UID  
			into dbo.DIMENSION_KEYS_PAGECASEID
 			  from  dbo.L_INV_ADMINISTRATIVE_INC  union
			 select L_INV_CLINICAL_INC.PAGE_CASE_UID 	 from  dbo.L_INV_CLINICAL_INC  union 
			 select L_INV_COMPLICATION_INC.PAGE_CASE_UID 	 from  dbo.L_INV_COMPLICATION_INC  union 
			 select L_INV_CONTACT_INC.PAGE_CASE_UID 	 from  dbo.L_INV_CONTACT_INC  union 
			 select L_INV_DEATH_INC.PAGE_CASE_UID 	 from  dbo.L_INV_DEATH_INC  union 
			 select L_INV_EPIDEMIOLOGY_INC.PAGE_CASE_UID 	 from  dbo.L_INV_EPIDEMIOLOGY_INC  union 
			 select L_INV_HIV_INC.PAGE_CASE_UID 	 from  dbo.L_INV_HIV_INC  union 
			 select L_INV_ISOLATE_TRACKING_INC.PAGE_CASE_UID 	 from  dbo.L_INV_ISOLATE_TRACKING_INC  union 
			 select L_INV_LAB_FINDING_INC.PAGE_CASE_UID 	 from  dbo.L_INV_LAB_FINDING_INC  union 
			 select L_INV_MEDICAL_HISTORY_INC.PAGE_CASE_UID 	 from  dbo.L_INV_MEDICAL_HISTORY_INC  union 
			 select L_INV_MOTHER_INC.PAGE_CASE_UID 	 from  dbo.L_INV_MOTHER_INC  union 
			 select L_INV_OTHER_INC.PAGE_CASE_UID 	 from  dbo.L_INV_OTHER_INC  union 
			 select L_INV_PATIENT_OBS_INC.PAGE_CASE_UID 	 from  dbo.L_INV_PATIENT_OBS_INC  union 
			 select L_INV_PREGNANCY_BIRTH_INC.PAGE_CASE_UID 	 from  dbo.L_INV_PREGNANCY_BIRTH_INC  union 
			 select L_INV_RESIDENCY_INC.PAGE_CASE_UID 	 from  dbo.L_INV_RESIDENCY_INC  union 
			 select L_INV_RISK_FACTOR_INC.PAGE_CASE_UID 	 from  dbo.L_INV_RISK_FACTOR_INC  union 
			 select L_INV_SOCIAL_HISTORY_INC.PAGE_CASE_UID 	 from  dbo.L_INV_SOCIAL_HISTORY_INC  union 
			 select L_INV_SYMPTOM_INC.PAGE_CASE_UID 	 from  dbo.L_INV_SYMPTOM_INC  union 
			 select L_INV_TRAVEL_INC.PAGE_CASE_UID 	 from  dbo.L_INV_TRAVEL_INC  union 
			 select L_INV_TREATMENT_INC.PAGE_CASE_UID 	 from  dbo.L_INV_TREATMENT_INC  union 
			 select L_INV_UNDER_CONDITION_INC.PAGE_CASE_UID 	 from  dbo.L_INV_UNDER_CONDITION_INC  union 
			 select L_INV_VACCINATION_INC.PAGE_CASE_UID 	 from  dbo.L_INV_VACCINATION_INC union  
			 SELECT L_INVESTIGATION_REPEAT_INC.PAGE_CASE_UID	 from  [dbo].[L_INVESTIGATION_REPEAT_INC] union  
			 SELECT L_INV_PLACE_REPEAT.PAGE_CASE_UID	 from  [dbo].[L_INV_PLACE_REPEAT]
			 ;	

		     SELECT @RowCount_no = @@ROWCOUNT;

		     INSERT INTO [dbo].[job_flow_log] 
				(batch_id,[Dataflow_Name],[package_Name] ,[Status_Type],[step_number],[step_name],[row_count])
				VALUES(@batch_id,'F_PAGE_CASE','F_PAGE_CASE','START',@Proc_Step_no,@Proc_Step_Name,@RowCount_no);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @Proc_Step_no = 6;
			SET @Proc_Step_Name = ' Generating DIMENSIONAL_KEYS'; 

			IF OBJECT_ID('dbo.DIMENSIONAL_KEYS', 'U') IS NOT NULL  
				 drop table dbo.DIMENSIONAL_KEYS
				 ;

			 select  DIMC.page_case_uid,   
 				 COALESCE(L_INV_ADMINISTRATIVE_INC.D_INV_ADMINISTRATIVE_KEY , 1) AS 	D_INV_ADMINISTRATIVE_KEY ,
				 COALESCE(L_INV_CLINICAL_INC.D_INV_CLINICAL_KEY , 1) AS 	D_INV_CLINICAL_KEY ,
				 COALESCE(L_INV_COMPLICATION_INC.D_INV_COMPLICATION_KEY , 1) AS 	D_INV_COMPLICATION_KEY ,
				 COALESCE(L_INV_CONTACT_INC.D_INV_CONTACT_KEY , 1) AS 	D_INV_CONTACT_KEY ,
				 COALESCE(L_INV_DEATH_INC.D_INV_DEATH_KEY , 1) AS 	D_INV_DEATH_KEY ,
				 COALESCE(L_INV_EPIDEMIOLOGY_INC.D_INV_EPIDEMIOLOGY_KEY , 1) AS 	D_INV_EPIDEMIOLOGY_KEY ,
				 COALESCE(L_INV_HIV_INC.D_INV_HIV_KEY , 1) AS 	D_INV_HIV_KEY ,
				 COALESCE(L_INV_PATIENT_OBS_INC.D_INV_PATIENT_OBS_KEY , 1) AS 	D_INV_PATIENT_OBS_KEY ,
				 COALESCE(L_INV_ISOLATE_TRACKING_INC.D_INV_ISOLATE_TRACKING_KEY , 1) AS 	D_INV_ISOLATE_TRACKING_KEY ,
				 COALESCE(L_INV_LAB_FINDING_INC.D_INV_LAB_FINDING_KEY , 1) AS 	D_INV_LAB_FINDING_KEY ,
				 COALESCE(L_INV_MEDICAL_HISTORY_INC.D_INV_MEDICAL_HISTORY_KEY , 1) AS 	D_INV_MEDICAL_HISTORY_KEY ,
				 COALESCE(L_INV_MOTHER_INC.D_INV_MOTHER_KEY , 1) AS 	D_INV_MOTHER_KEY ,
				 COALESCE(L_INV_OTHER_INC.D_INV_OTHER_KEY , 1) AS 	D_INV_OTHER_KEY ,
				 COALESCE(L_INV_PREGNANCY_BIRTH_INC.D_INV_PREGNANCY_BIRTH_KEY , 1) AS 	D_INV_PREGNANCY_BIRTH_KEY ,
				 COALESCE(L_INV_RESIDENCY_INC.D_INV_RESIDENCY_KEY , 1) AS 	D_INV_RESIDENCY_KEY ,
				 COALESCE(L_INV_RISK_FACTOR_INC.D_INV_RISK_FACTOR_KEY , 1) AS 	D_INV_RISK_FACTOR_KEY ,
				 COALESCE(L_INV_SOCIAL_HISTORY_INC.D_INV_SOCIAL_HISTORY_KEY , 1) AS 	D_INV_SOCIAL_HISTORY_KEY ,
				 COALESCE(L_INV_SYMPTOM_INC.D_INV_SYMPTOM_KEY , 1) AS 	D_INV_SYMPTOM_KEY ,
				 COALESCE(L_INV_TREATMENT_INC.D_INV_TREATMENT_KEY , 1) AS 	D_INV_TREATMENT_KEY ,
				 COALESCE(L_INV_TRAVEL_INC.D_INV_TRAVEL_KEY , 1) AS 	D_INV_TRAVEL_KEY ,
				 COALESCE(L_INV_UNDER_CONDITION_INC.D_INV_UNDER_CONDITION_KEY , 1) AS 	D_INV_UNDER_CONDITION_KEY ,
				 COALESCE(L_INV_VACCINATION_INC.D_INV_VACCINATION_KEY , 1) AS 	D_INV_VACCINATION_KEY ,
				 COALESCE(L_INVESTIGATION_REPEAT_INC.D_INVESTIGATION_REPEAT_KEY , 1 ) AS	D_INVESTIGATION_REPEAT_KEY,
				 COALESCE(L_INV_PLACE_REPEAT.D_INV_PLACE_REPEAT_KEY , 1 ) AS	D_INV_PLACE_REPEAT_KEY
			
                into dbo.DIMENSIONAL_KEYS	     
				from dbo.DIMENSION_KEYS_PAGECASEID DIMC
					  LEFT OUTER JOIN   dbo.L_INV_ADMINISTRATIVE_INC ON  L_INV_ADMINISTRATIVE_INC.PAGE_CASE_UID  =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_CLINICAL_INC ON  L_INV_CLINICAL_INC.PAGE_CASE_UID  =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_COMPLICATION_INC ON  L_INV_COMPLICATION_INC.PAGE_CASE_UID  =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_CONTACT_INC ON  L_INV_CONTACT_INC.PAGE_CASE_UID  =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_DEATH_INC ON  L_INV_DEATH_INC.PAGE_CASE_UID  =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_EPIDEMIOLOGY_INC ON  L_INV_EPIDEMIOLOGY_INC.PAGE_CASE_UID  =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_HIV_INC ON  L_INV_HIV_INC.PAGE_CASE_UID  =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_ISOLATE_TRACKING_INC ON  L_INV_ISOLATE_TRACKING_INC.PAGE_CASE_UID  =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_LAB_FINDING_INC ON  L_INV_LAB_FINDING_INC.PAGE_CASE_UID  =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_MEDICAL_HISTORY_INC ON  L_INV_MEDICAL_HISTORY_INC.PAGE_CASE_UID  =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_MOTHER_INC ON  L_INV_MOTHER_INC.PAGE_CASE_UID  =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_OTHER_INC ON  L_INV_OTHER_INC.PAGE_CASE_UID = dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_PATIENT_OBS_INC ON  L_INV_PATIENT_OBS_INC.PAGE_CASE_UID  =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_PREGNANCY_BIRTH_INC ON  L_INV_PREGNANCY_BIRTH_INC.PAGE_CASE_UID  =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_RESIDENCY_INC ON  L_INV_RESIDENCY_INC.PAGE_CASE_UID  =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_RISK_FACTOR_INC ON  L_INV_RISK_FACTOR_INC.PAGE_CASE_UID  =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_SOCIAL_HISTORY_INC ON  L_INV_SOCIAL_HISTORY_INC.PAGE_CASE_UID  =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_SYMPTOM_INC ON  L_INV_SYMPTOM_INC.PAGE_CASE_UID  =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_TRAVEL_INC ON  L_INV_TRAVEL_INC.PAGE_CASE_UID  =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_TREATMENT_INC ON   L_INV_TREATMENT_INC.PAGE_CASE_UID  =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_UNDER_CONDITION_INC ON   L_INV_UNDER_CONDITION_INC.PAGE_CASE_UID  =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_VACCINATION_INC ON  L_INV_VACCINATION_INC.PAGE_CASE_UID  =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INVESTIGATION_REPEAT_INC ON  L_INVESTIGATION_REPEAT_INC.PAGE_CASE_UID =  dimc.page_case_uid
					  LEFT OUTER JOIN   dbo.L_INV_PLACE_REPEAT ON  L_INV_PLACE_REPEAT.PAGE_CASE_UID =  dimc.page_case_uid
					   where  L_INV_ADMINISTRATIVE_INC.PAGE_CASE_UID IN (SELECT PAGE_CASE_UID FROM dbo.PHC_UIDS WHERE CASE_MANAGEMENT_UID IS NULL)
                   ;

		     SELECT @RowCount_no = @@ROWCOUNT;

		  INSERT INTO [dbo].[job_flow_log] 
				(batch_id,[Dataflow_Name],[package_Name] ,[Status_Type],[step_number],[step_name],[row_count])
				VALUES(@batch_id,'F_PAGE_CASE','F_PAGE_CASE','START',@Proc_Step_no,@Proc_Step_Name,@RowCount_no);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @Proc_Step_no = 7;
			SET @Proc_Step_Name = ' Generating F_PAGE_CASE_TEMP_INC'; 

			IF OBJECT_ID('dbo.F_PAGE_CASE_TEMP_INC', 'U') IS NOT NULL  
				 drop table dbo.F_PAGE_CASE_TEMP_INC
				 ;


		--DROP TABLE dbo.F_PAGE_CASE;

		--CREATE TABLE 	F_PAGE_CASE AS 
		SELECT     
			DIM_KEYS.*, 
			--CAST ( 1 as float ) AS D_INV_PLACE_REPEAT_KEY,
			KEYSTORE.CONDITION_KEY,
			KEYSTORE.INVESTIGATION_KEY,
			KEYSTORE.PHYSICIAN_KEY,
			KEYSTORE.INVESTIGATOR_KEY,
			KEYSTORE.HOSPITAL_KEY as HOSPITAL_KEY,
			KEYSTORE.PATIENT_KEY,
			KEYSTORE.PERSON_AS_REPORTER_KEY AS PERSON_AS_REPORTER_KEY,
			KEYSTORE.ORG_AS_REPORTER_KEY AS ORG_AS_REPORTER_KEY,
			--KEYSTORE.HOSPITAL_KEY AS HOSPITAL_KEY,
			KEYSTORE.GEOCODING_LOCATION_KEY,
			DATE1.DATE_KEY AS ADD_DATE_KEY, 
			DATE2.DATE_KEY AS LAST_CHG_DATE_KEY
			
		INTO dbo.F_PAGE_CASE_TEMP_INC
		FROM  dbo.DIMENSIONAL_KEYS as DIM_KEYS
				 INNER JOIN dbo.ENTITY_KEYSTORE_INC AS KEYSTORE ON DIM_KEYS.PAGE_CASE_UID = KEYSTORE.PAGE_CASE_UID 
				  LEFT OUTER JOIN dbo.RDB_DATE DATE1 ON cast(DATE1.DATE_MM_DD_YYYY as date)= cast(KEYSTORE.ADD_TIME as date)
				  LEFT OUTER JOIN dbo.RDB_DATE DATE2 ON cast(DATE2.DATE_MM_DD_YYYY as date )=cast(KEYSTORE.LAST_CHG_TIME as date)
		;

				--?? LEFT OUTER JOIN dbo.RDB_DATE DATE1 ON DATEPART(DATE1.DATE_MM_DD_YYYY)=DATEPART(KEYSTORE.ADD_TIME)
				 --??LEFT OUTER JOIN dbo.RDB_DATE DATE2 ON DATEPART(DATE2.DATE_MM_DD_YYYY)=DATEPART(KEYSTORE.LAST_CHG_TIME)
		 


	
		/*
		DATA F_PAGE_CASE;
		SET F_PAGE_CASE;
		IF D_F_PAGE_CASE_KEY= . THEN D_F_PAGE_CASE_KEY=1;
		IF D_INV_PLACE_REPEAT_KEY=. THEN D_INV_PLACE_REPEAT_KEY=1;
		*/


		     SELECT @RowCount_no = @@ROWCOUNT;

		     INSERT INTO [dbo].[job_flow_log] 
				(batch_id,[Dataflow_Name],[package_Name] ,[Status_Type],[step_number],[step_name],[row_count])
				VALUES(@batch_id,'F_PAGE_CASE','F_PAGE_CASE','START',@Proc_Step_no,@Proc_Step_Name,@RowCount_no);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @Proc_Step_no = 8;
			SET @Proc_Step_Name = ' Generating DROP COLUMNS'; 

		-- DROP COLUMN PAGE_CASE_UID;
        	ALTER TABLE  dbo.F_PAGE_CASE_TEMP_INC DROP COLUMN PAGE_CASE_UID ;  
        
		--??PROC SORT DATA=F_PAGE_CASE NODUPKEY; BY PATIENT_KEY; RUN;

			IF OBJECT_ID('dbo.F_PAGE_CASE', 'U') IS NOT NULL  
			BEGIN
					 --drop table dbo.F_PAGE_CASE;
					 DELETE fpagecase FROM dbo.F_PAGE_CASE fpagecase
					 JOIN dbo.F_PAGE_CASE_TEMP_INC fpagecaseinc ON fpagecase.investigation_key=fpagecaseinc.investigation_key;
	
					 INSERT INTO dbo.F_PAGE_CASE SELECT * FROM dbo.F_PAGE_CASE_TEMP_INC;
			END;

			
			IF OBJECT_ID('dbo.F_PAGE_CASE', 'U') IS NULL 
			BEGIN
				SELECT * 
				into [dbo].F_PAGE_CASE
				FROM
				(
				  SELECT *, 
					ROW_NUMBER () OVER (PARTITION BY PATIENT_KEY order by PATIENT_KEY) rowid
						FROM [dbo].F_PAGE_CASE_TEMP_INC 
				  ) AS Der WHERE rowid=1;


				ALTER TABLE  dbo.F_PAGE_CASE DROP COLUMN rowid ;  
			END;
			/**
			This should cover any issue with defect https://nbscentral.sramanaged.com/redmine/issues/12555
			ETL Error in Dynamic Datamarts Process - Problem Record(s) Causing Million+ Rows in Dynamic Datamart (Total Should Be a Few Thousand)
			*/
			
			DELETE FROM [DBO].F_PAGE_CASE WHERE INVESTIGATION_KEY IN (SELECT INVESTIGATION_KEY FROM F_PAGE_CASE 
				GROUP BY INVESTIGATION_KEY HAVING COUNT(INVESTIGATION_KEY)>1) AND PATIENT_KEY =1

		     INSERT INTO [dbo].[job_flow_log] 
				(batch_id,[Dataflow_Name],[package_Name] ,[Status_Type],[step_number],[step_name],[row_count])
				VALUES(@batch_id,'F_PAGE_CASE','F_PAGE_CASE','START',@Proc_Step_no,@Proc_Step_Name,@RowCount_no);  

		
    COMMIT TRANSACTION;
   
   	/** TODO: (Upasana) Commented for Change Data Capture- End: Processing logic **/
	UPDATE landing
	SET landing.cdc_status =
		CASE 
			WHEN session_table.cdc_id = landing.cdc_id AND session_table.cdc_topic_updated_datetime=landing.cdc_topic_updated_datetime THEN 1
			ELSE  0
		END,
	landing.cdc_processed_datetime = GETDATE(),
	landing.cdc_status_desc = 'F_PAGE_CASE'
	FROM nbs_changedata.dbo.case_management landing
		INNER JOIN #TMP_CDC_Case_management session_table ON landing.case_management_uid = session_table.case_management_uid AND landing.cdc_id = session_table.cdc_id
	
	UPDATE landing
	SET landing.cdc_status =
		CASE 
			WHEN session_table.cdc_id = landing.cdc_id AND session_table.cdc_topic_updated_datetime=landing.cdc_topic_updated_datetime THEN 1
			ELSE  0
		END,
	landing.cdc_processed_datetime = GETDATE(),
	landing.cdc_status_desc = 'F_PAGE_CASE'
	FROM nbs_changedata.dbo.Public_health_case landing
		INNER JOIN #TMP_CDC_Public_health_case session_table ON landing.public_health_case_uid = session_table.public_health_case_uid AND landing.cdc_id = session_table.cdc_id
	
	UPDATE landing
	SET landing.cdc_status =
		CASE 
			WHEN session_table.cdc_id = landing.cdc_id AND session_table.cdc_topic_updated_datetime=landing.cdc_topic_updated_datetime THEN 1
			ELSE  0
		END,
	landing.cdc_processed_datetime = GETDATE(),
	landing.cdc_status_desc = 'F_PAGE_CASE'
	FROM nbs_changedata.dbo.NBS_act_entity landing
		INNER JOIN #TMP_CDC_NBS_act_entity session_table ON landing.nbs_act_entity_uid = session_table.nbs_act_entity_uid AND landing.cdc_id = session_table.cdc_id
			
	

    BEGIN TRANSACTION ;
	
	SET @Proc_Step_no = 9;
	SET @Proc_Step_Name = 'SP_COMPLETE'; 


	INSERT INTO [dbo].[job_flow_log] (
		    batch_id
			,[Dataflow_Name]
		   ,[package_Name]
		    ,[Status_Type] 
           ,[step_number]
           ,[step_name]
           ,[row_count]
           )
		   VALUES
           (
		   @batch_id,
           'F_PAGE_CASE'
           ,'S_F_PAGE_CASE'
		   ,'COMPLETE'
		   ,@Proc_Step_no
		   ,@Proc_Step_name
           ,@RowCount_no
		   );
  
	
	COMMIT TRANSACTION;
  END TRY

  BEGIN CATCH
  
     
     IF @@TRANCOUNT > 0   ROLLBACK TRANSACTION;
 
  
	
	DECLARE @ErrorNumber INT = ERROR_NUMBER();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
    DECLARE @ErrorState INT = ERROR_STATE();
 
	
    INSERT INTO [dbo].[job_flow_log] (
		    batch_id
		   ,[Dataflow_Name]
		   ,[package_Name]
		    ,[Status_Type] 
           ,[step_number]
           ,[step_name]
           ,[Error_Description]
		   ,[row_count]
           )
		   VALUES
           (
           @batch_id
           ,'F_PAGE_CASE'
           ,'S_F_PAGE_CASE'
		   ,'ERROR'
		   ,@Proc_Step_no
		   ,'ERROR - '+ @Proc_Step_name
           , 'Step -' +CAST(@Proc_Step_no AS VARCHAR(3))+' -' +CAST(@ErrorMessage AS VARCHAR(500))
           ,0
		   );
  

      return -1 ;

	END CATCH
	
END

;


GO
