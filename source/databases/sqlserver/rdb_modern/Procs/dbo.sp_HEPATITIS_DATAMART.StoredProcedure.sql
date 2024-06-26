USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[sp_HEPATITIS_DATAMART]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[sp_HEPATITIS_DATAMART] 
				 @batch_id bigint
AS
BEGIN

	-----EXEC  sp_HEPATITIS_DATAMART 12345
	--Delete from [dbo].[job_flow_log] where batch_id =12345
	--Select * from [dbo].[job_flow_log] where batch_id =12345
	---DECLARE @batch_id BIGINT = 12345

	DECLARE @RowCount_no int;
	DECLARE @Proc_Step_no float= 0;
	DECLARE @Proc_Step_Name varchar(200)= '';
	DECLARE @batch_start_time datetime2(7)= NULL;
	DECLARE @batch_end_time datetime2(7)= NULL;
	DECLARE @COUNT_PB_HEP AS int;
	DECLARE @date_last_run datetime2(7)= NULL;
	SET @COUNT_PB_HEP =
	(
		SELECT COUNT(*)
		FROM nbs_odse.dbo.NBS_ui_metadata WITH(NOLOCK)
		WHERE investigation_form_cd IN
		(
			SELECT investigation_form_cd
			FROM NBS_SRTE.dbo.[Condition_code] WITH(NOLOCK)
			WHERE CONDITION_CD IN( '10110', '10104', '10100', '10106', '10101', '10102', '10103', '10105', '10481', '50248', '999999' )
		)
	);
	BEGIN TRY
		BEGIN TRANSACTION;
		IF(@COUNT_PB_HEP > 0)
		BEGIN
			SET @Proc_Step_no = 1;
			SET @Proc_Step_Name = 'SP_Start';
			BEGIN TRANSACTION;
			INSERT INTO [dbo].[job_flow_log]( batch_id, ---------------@batch_id 
			[Dataflow_Name], --------------'Hepatitis_Case_DATAMART' 
			[package_Name], --------------'Hepatitis' 
			[Status_Type], ---------------START 
			[step_number], ---------------@Proc_Step_no 
			[step_name], ------------------@Proc_Step_Name=sp_start 
			[row_count] --------------------0
			)
			VALUES( @batch_id, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @Proc_Step_no, @Proc_Step_Name, 0 );
			COMMIT TRANSACTION;
			SELECT @batch_start_time = batch_start_dttm, @batch_end_time = batch_end_dttm
			FROM [dbo].[job_batch_log]
			WHERE status_type = 'start';

			---------------------------------------------------------------------2 Create Table HEPATITIS_DATAMART_LAST-----------------------

			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating  HEPATITIS_DATAMART_LAST ';
			SET @Proc_Step_no = 2;
			IF OBJECT_ID('dbo.HEPATITIS_DATAMART_LAST', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.HEPATITIS_DATAMART_LAST;
			END;
			CREATE TABLE dbo.HEPATITIS_DATAMART_LAST
			( 
						 date_last_ran date, START_DATE date
			);
			INSERT INTO dbo.HEPATITIS_DATAMART_LAST( date_last_ran, START_DATE )
			VALUES( '01jun1900', '01jun1900' );
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			UPDATE dbo.HEPATITIS_DATAMART_LAST
			  SET date_last_ran =
			(
				SELECT CAST(MAX([REFRESH_DATETIME]) AS date)
				FROM [dbo].[HEPATITIS_DATAMART]
			), START_DATE = GETDATE();
			UPDATE dbo.HEPATITIS_DATAMART_LAST
			  SET date_last_ran = '01jun1900'
			WHERE date_last_ran IS NULL;
			SET @date_last_run =
			(
				SELECT date_last_ran
				FROM dbo.HEPATITIS_DATAMART_LAST
			);
			COMMIT TRANSACTION;
								

			-----------------------------------------------------------3. Create Table dbo.Update_Patient_Cases-------------
			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating Update_Patient_Cases';
			SET @Proc_Step_no = 3;
			IF OBJECT_ID('dbo.Update_Patient_Cases', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.Update_Patient_Cases;
			END;
			SELECT P.PATIENT_LAST_CHANGE_TIME
			INTO dbo.Update_Patient_Cases
			FROM [dbo].[D_PATIENT] AS p WITH(NOLOCK), [dbo].[F_PAGE_CASE] WITH(NOLOCK)
			WHERE P.PATIENT_KEY = F_PAGE_CASE.PATIENT_KEY AND 
				  p.PATIENT_LAST_CHANGE_TIME > @date_last_run;
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;

			-----------------------------------------------------------4. Create Table dbo.Updated_Hep_Patient-------------

			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating Updated_Hep_Patient';
			SET @Proc_Step_no = 4;
			IF OBJECT_ID('dbo.Updated_Hep_Patient', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.Updated_Hep_Patient;
			END;
			SELECT DISTINCT 
				   P.PATIENT_UID
			INTO dbo.Updated_Hep_Patient
			FROM dbo.D_PATIENT AS p WITH(NOLOCK), dbo.HEPATITIS_DATAMART WITH(NOLOCK)
			WHERE P.PATIENT_UID = HEPATITIS_DATAMART.PATIENT_UID AND 
				  PATIENT_LAST_CHANGE_TIME > @date_last_run;
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;

			-----------------------------------------------------------5. Create Table dbo.Updated_Hep_PHYSICIAN-------------

			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating Updated_Hep_PHYSICIAN';
			SET @Proc_Step_no = 5;
			IF OBJECT_ID('dbo.Updated_Hep_PHYSICIAN', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.Updated_Hep_PHYSICIAN;
			END;
			SELECT DISTINCT 
				   P.PROVIDER_UID AS PHYSICIAN_UID
			INTO dbo.Updated_Hep_PHYSICIAN
			FROM [dbo].[D_PROVIDER] AS p WITH(NOLOCK), [dbo].[HEPATITIS_DATAMART] WITH(NOLOCK)
			WHERE P.PROVIDER_UID = HEPATITIS_DATAMART.PHYSICIAN_UID AND 
				  CAST(PROVIDER_LAST_CHANGE_TIME AS date) > @date_last_run;
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;

			--------------------------------------------------------6. Create Table dbo.Updated_Hep_INVESTIGATOR-------------

			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating dbo.Updated_Hep_INVESTIGATOR';
			SET @Proc_Step_no = 6;
			IF OBJECT_ID('dbo.Updated_Hep_INVESTIGATOR', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.Updated_Hep_INVESTIGATOR;
			END;
			SELECT DISTINCT 
				   p.PROVIDER_UID AS INVESTIGATOR_UID
			INTO dbo.Updated_Hep_INVESTIGATOR
			FROM [dbo].[D_PROVIDER] AS p WITH(NOLOCK), [dbo].[HEPATITIS_DATAMART] WITH(NOLOCK)
			WHERE P.PROVIDER_UID = HEPATITIS_DATAMART.INVESTIGATOR_UID AND 
				  PROVIDER_LAST_CHANGE_TIME > @date_last_run;
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;
			-----------------------------------------------------------7. Create Table dbo.Updated_Hep_REPORTING-------------
			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating  dbo.Updated_Hep_REPORTING';
			SET @Proc_Step_no = 7;
			IF OBJECT_ID('dbo.Updated_Hep_REPORTING', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.Updated_Hep_REPORTING;
			END;
			-----modified below on 9/29/2021
			/*SELECT P.ORGANIZATION_UID AS REPORTING_SOURCE_UID, ORGANIZATION_NAME, ORGANIZATION_COUNTY_CODE, ORGANIZATION_COUNTY, ORGANIZATION_CITY
			INTO dbo.Updated_Hep_REPORTING
			FROM [dbo].[D_ORGANIZATION] AS p WITH(NOLOCK), [dbo].[HEPATITIS_DATAMART] WITH(NOLOCK)
			WHERE P.ORGANIZATION_UID = HEPATITIS_DATAMART.REPORTING_SOURCE_UID AND 
				  CAST(p.ORGANIZATION_LAST_CHANGE_TIME AS date) > @date_last_run;*/

			SELECT P.ORGANIZATION_UID AS REPORTING_SOURCE_UID, ORGANIZATION_NAME, ORGANIZATION_COUNTY_CODE, ORGANIZATION_COUNTY, ORGANIZATION_CITY
			INTO dbo.Updated_Hep_REPORTING
			FROM [dbo].[D_ORGANIZATION] AS p WITH(NOLOCK), [dbo].[HEPATITIS_DATAMART] WITH(NOLOCK)
			WHERE P.ORGANIZATION_UID = HEPATITIS_DATAMART.REPORTING_SOURCE_UID AND 
				  ORGANIZATION_LAST_CHANGE_TIME > @date_last_run;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;

			---------------------------------------------------------------------------------------------------------------------------------------------
			BEGIN
				DELETE FROM dbo.HEPATITIS_DATAMART
				WHERE PATIENT_UID IN
				(
					SELECT DISTINCT 
						   PATIENT_UID
					FROM dbo.updated_hep_PATIENT
				);
				DELETE FROM dbo.HEPATITIS_DATAMART
				WHERE PHYSICIAN_UID IN
				(
					SELECT DISTINCT 
						   PHYSICIAN_UID
					FROM dbo.updated_hep_PHYSICIAN
				);
				DELETE FROM dbo.HEPATITIS_DATAMART
				WHERE INVESTIGATOR_UID IN
				(
					SELECT DISTINCT 
						   INVESTIGATOR_UID
					FROM dbo.updated_hep_INVESTIGATOR
				);
				DELETE FROM dbo.HEPATITIS_DATAMART
				WHERE REPORTING_SOURCE_UID IN
				(
					SELECT DISTINCT 
						   REPORTING_SOURCE_UID
					FROM dbo.updated_hep_REPORTING
				);
			END;
			-----------------------------------------------------------8. Create Table dbo.EXISTING_HEPATITIS_DATAMART-------------
			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating  dbo.EXISTING_HEPATITIS_DATAMART';
			SET @Proc_Step_no = 8;
			IF OBJECT_ID('dbo.EXISTING_HEPATITIS_DATAMART', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.EXISTING_HEPATITIS_DATAMART;
			END;
			SELECT DISTINCT 
				   investigation.investigation_key
			INTO dbo.EXISTING_HEPATITIS_DATAMART
			FROM dbo.HEPATITIS_DATAMART WITH(NOLOCK), dbo.INVESTIGATION WITH(NOLOCK)
			WHERE dbo.HEPATITIS_DATAMART.[CASE_UID] = INVESTIGATION.[CASE_UID];
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;

			-----------------------------------------------------------9. Create Table dbo.TMP_CONDITION---------------------------------------------------
			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating  dbo.TMP_CONDITION';
			SET @Proc_Step_no = 9;
			IF OBJECT_ID('dbo.TMP_CONDITION', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_CONDITION;
			END;
			SELECT CONDITION_CD, CONDITION_DESC, DISEASE_GRP_DESC, CONDITION_KEY
			INTO dbo.TMP_CONDITION
			FROM [dbo].[CONDITION] WITH(NOLOCK)
			WHERE CONDITION_CD IN( '10110', '10104', '10100', '10106', '10101', '10102', '10103', '10105', '10481', '50248', '999999' );
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;

			---------------------------------------------------------------------10. CREATE TABLE dbo.TMP_F_PAGE_CASE-------------------------------------

			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating  dbo.TMP_F_PAGE_CASE';
			SET @Proc_Step_no = 10;
			IF OBJECT_ID('dbo.TMP_F_PAGE_CASE', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_F_PAGE_CASE;
			END;
			SELECT F_PAGE_CASE.INVESTIGATION_KEY, T.CONDITION_KEY, F_PAGE_CASE.PATIENT_KEY
			INTO TMP_F_PAGE_CASE
			FROM [dbo].[F_PAGE_CASE] WITH(NOLOCK)---Original table 
				 INNER JOIN
				 dbo.TMP_CONDITION AS T WITH(NOLOCK)
				 ON F_PAGE_CASE.CONDITION_KEY = T.CONDITION_KEY  ------(my table comdition)
				 INNER JOIN
				 [dbo].[D_PATIENT] WITH(NOLOCK)
				 ON F_PAGE_CASE.PATIENT_KEY = D_PATIENT.PATIENT_KEY
				 INNER JOIN
				 dbo.INVESTIGATION WITH(NOLOCK)
				 ON INVESTIGATION.INVESTIGATION_KEY = F_PAGE_CASE.INVESTIGATION_KEY
				 LEFT JOIN
				 EXISTING_HEPATITIS_DATAMART WITH(NOLOCK)
				 ON F_PAGE_CASE.INVESTIGATION_KEY = EXISTING_HEPATITIS_DATAMART.INVESTIGATION_KEY
			WHERE EXISTING_HEPATITIS_DATAMART.INVESTIGATION_KEY IS NULL AND 
				  INVESTIGATION.RECORD_STATUS_CD = 'ACTIVE'
			ORDER BY F_PAGE_CASE.INVESTIGATION_KEY;
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;

			---------------------------------------------------------------------11. CREATE TABLE dbo.TMP_D_INV_ADMINISTRATIVE
			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating  dbo.TMP_D_INV_ADMINISTRATIVE';
			SET @Proc_Step_no = 11;
			IF OBJECT_ID('dbo.TMP_F_INV_ADMINISTRATIVE', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_F_INV_ADMINISTRATIVE;
			END;
			IF OBJECT_ID('dbo.TMP_D_INV_ADMINISTRATIVE', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_INV_ADMINISTRATIVE;
			END;

			SELECT page_case.D_INV_ADMINISTRATIVE_KEY, page_case.INVESTIGATION_KEY
			INTO dbo.TMP_F_INV_ADMINISTRATIVE
			FROM dbo.F_PAGE_CASE AS page_case WITH(NOLOCK) ----Original table
				 INNER JOIN
				 TMP_F_PAGE_CASE AS T
				 ON T.INVESTIGATION_KEY = PAGE_CASE.INVESTIGATION_KEY  ---(My Table)--Should it be F-Page or tmp_F_Page
				 ORDER BY D_INV_ADMINISTRATIVE_KEY;

            declare @SQL varchar(2000)
			set @SQL =
			'SELECT F.D_INV_ADMINISTRATIVE_KEY, F.INVESTIGATION_KEY AS ADMIN_INV_KEY, ADM_INNC_NOTIFICATION_DT AS INIT_NND_NOT_DT, ADM_FIRST_RPT_TO_PHD_DT AS FIRST_RPT_PHD_DT, ADM_BINATIONAL_RPTNG_CRIT AS BINATIONAL_RPTNG_CRIT
			INTO dbo.TMP_D_INV_ADMINISTRATIVE
			FROM dbo.TMP_F_INV_ADMINISTRATIVE AS F
				 LEFT JOIN
				 dbo.D_INV_ADMINISTRATIVE AS D WITH(NOLOCK)
				 ON F.D_INV_ADMINISTRATIVE_KEY = D.D_INV_ADMINISTRATIVE_KEY---in1=1===Left Join
				 ORDER BY F.INVESTIGATION_KEY';
             exec (@SQL);
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;
			-----------------------------------------------------------------------------12. CREATE TABLE TMP_D_INV_CLINICAL----------------------------
			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating  dbo.TMP_D_INV_CLINICAL';
			SET @Proc_Step_no = 12;
			IF OBJECT_ID('dbo.TMP_F_INV_CLINICAL', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_F_INV_CLINICAL;
			END;
			IF OBJECT_ID('dbo.TMP_D_INV_CLINICAL', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_INV_CLINICAL;
			END;
			SELECT F_PAGE_CASE.D_INV_CLINICAL_KEY, F_PAGE_CASE.INVESTIGATION_KEY
			INTO dbo.TMP_F_INV_CLINICAL
			FROM dbo.F_PAGE_CASE WITH(NOLOCK)
				 INNER JOIN
				 TMP_F_PAGE_CASE AS PAGE_CASE
				 ON F_PAGE_CASE.INVESTIGATION_KEY = PAGE_CASE.INVESTIGATION_KEY ---(my Table)
				 ORDER BY D_INV_CLINICAL_KEY;
			SELECT F.D_INV_CLINICAL_KEY, F.INVESTIGATION_KEY AS CLINICAL_INV_KEY, CAST(CLN_HepDInfection AS varchar(300)) AS HEP_D_INFECTION_IND, CAST(CLN_MedsforHep AS varchar(300)) AS HEP_MEDS_RECVD_IND
			INTO dbo.TMP_D_INV_CLINICAL
			FROM dbo.TMP_F_INV_CLINICAL AS F
				 LEFT JOIN
				 [dbo].[D_INV_CLINICAL] AS D WITH(NOLOCK)
				 ON F.D_INV_CLINICAL_KEY = D.D_INV_CLINICAL_KEY
				 ORDER BY F.INVESTIGATION_KEY;
			/*UPDATE dbo.TMP_D_INV_CLINICAL
			  SET HEP_D_INFECTION_IND = ( CASE
										  WHEN HEP_D_INFECTION_IND IS NULL THEN NULL
										  ELSE RTRIM(LTRIM(HEP_D_INFECTION_IND))
										  END );
			UPDATE dbo.TMP_D_INV_CLINICAL
			  SET HEP_MEDS_RECVD_IND = ( CASE
										 WHEN HEP_MEDS_RECVD_IND IS NULL THEN NULL
										 ELSE RTRIM(LTRIM(HEP_MEDS_RECVD_IND))
										 END );
										 */
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;
			--------------------------------------------------------------------------------13. CREATE TABLE TMP_D_INV_PATIENT_OBS----------------------------
			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating  dbo.TMP_D_INV_PATIENT_OBS';
			SET @Proc_Step_no = 13;
			IF OBJECT_ID('dbo.TMP_D_INV_PATIENT_OBS', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_INV_PATIENT_OBS;
			END;
			IF OBJECT_ID('dbo.TMP_F_INV_PATIENT_OBS', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_F_INV_PATIENT_OBS;
			END;
			SELECT F_PAGE_CASE.D_INV_PATIENT_OBS_KEY, F_PAGE_CASE.INVESTIGATION_KEY
			INTO dbo.TMP_F_INV_PATIENT_OBS
			FROM dbo.F_PAGE_CASE WITH(NOLOCK)
				 INNER JOIN
				 TMP_F_PAGE_CASE AS PAGE_CASE
				 ON F_PAGE_CASE.INVESTIGATION_KEY = PAGE_CASE.INVESTIGATION_KEY	---(my table)
				 ORDER BY D_INV_PATIENT_OBS_KEY;
			SELECT F.D_INV_PATIENT_OBS_KEY, F.INVESTIGATION_KEY AS PATIENT_OBS_INV_KEY, CAST(IPO_SEXUAL_PREF AS varchar(300)) AS SEX_PREF
			INTO dbo.TMP_D_INV_PATIENT_OBS
			FROM dbo.TMP_F_INV_PATIENT_OBS AS F
				 LEFT JOIN
				 [dbo].[D_INV_PATIENT_OBS] AS D WITH(NOLOCK)
				 ON F.D_INV_PATIENT_OBS_KEY = D.D_INV_PATIENT_OBS_KEY
				 ORDER BY F.INVESTIGATION_KEY;
			/*UPDATE dbo.TMP_D_INV_PATIENT_OBS
			  SET SEX_PREF = ( CASE
							   WHEN SEX_PREF IS NULL THEN NULL
							   ELSE RTRIM(LTRIM(SEX_PREF))
							   END );*/
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;
			--------------------------------------------------------------------------------14. CREATE TABLE TMP_D_INV_EPIDEMIOLOGY----------------------------
			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating  dbo.TMP_D_INV_EPIDEMIOLOGY';
			SET @Proc_Step_no = 14;
			IF OBJECT_ID('dbo.TMP_F_INV_EPIDEMIOLOGY', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_F_INV_EPIDEMIOLOGY;
			END;
			IF OBJECT_ID('dbo.TMP_D_INV_EPIDEMIOLOGY', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_INV_EPIDEMIOLOGY;
			END;
			SELECT F_PAGE_CASE.[D_INV_EPIDEMIOLOGY_KEY], F_PAGE_CASE.[INVESTIGATION_KEY]
			INTO dbo.TMP_F_INV_EPIDEMIOLOGY
			FROM dbo.F_PAGE_CASE WITH(NOLOCK)
				 INNER JOIN
				 TMP_F_PAGE_CASE AS PAGE_CASE
				 ON F_PAGE_CASE.INVESTIGATION_KEY = PAGE_CASE.INVESTIGATION_KEY	 ---(my table)
				 ORDER BY D_INV_EPIDEMIOLOGY_KEY;
			SELECT F.D_INV_EPIDEMIOLOGY_KEY, F.INVESTIGATION_KEY AS EPIDEMIOLOGY_INV_KEY, CAST(EPI_ChildCareCase AS varchar(300)) AS CHILDCARE_CASE_IND, ----1
			CAST(EPI_CNTRY_USUAL_RESID AS varchar(300)) AS CNTRY_USUAL_RESIDENCE, --2
			CAST(EPI_ContactBabysitter AS varchar(300)) AS CT_BABYSITTER_IND, ----3
			CAST(EPI_ContactChildcare AS varchar(300)) AS CT_CHILDCARE_IND, --4
			CAST(EPI_ContactHousehold AS varchar(300)) AS CT_HOUSEHOLD_IND, --5
			CAST(EPI_ContactOfCase AS varchar(300)) AS HEP_CONTACT_IND, ----6
			CAST(EPI_ContactOther AS varchar(300)) AS OTHER_CONTACT_IND, -----7
			CAST(EPI_ContactOthSpecify AS varchar(300)) AS CONTACT_TYPE_OTH, --8
			CAST(EPI_ContactPlaymate AS varchar(300)) AS CT_PLAYMATE_IND, ----9
			CAST(EPI_ContactSexPartner AS varchar(300)) AS SEXUAL_PARTNER_IND, ---10

			CAST(EPI_DaycareContact AS varchar(300)) AS DNP_HOUSEHOLD_CT_IND, -----11
			CAST(EPI_EpiLinked AS varchar(300)) AS HEP_A_EPLINK_IND, ----------12
			CAST(EPI_FemaleSexPartners AS varchar(300)) AS FEMALE_SEX_PRTNR_NBR, ------13
			CAST(EPI_FoodHandler AS varchar(300)) AS FOODHNDLR_PRIOR_IND, ------14
			CAST(EPI_InDayCare AS varchar(300)) AS DNP_EMPLOYEE_IND, ----------15
			CAST(EPI_IVDrugUse AS varchar(300)) AS STREET_DRUG_INJECTED, ------16
			CAST(EPI_MaleSexPartner AS varchar(300)) AS MALE_SEX_PRTNR_NBR, -------17
			--- EPI_OutbreakAssoc,	   ---OUTBREAK_IND-----18
			CAST(EPI_OutbreakFoodHndlr AS varchar(300)) AS OBRK_FOODHNDLR_IND, -------19
			CAST(EPI_OutbreakFoodItem AS varchar(300)) AS FOOD_OBRK_FOOD_ITEM, ------20
			CAST(EPI_outbreakNonFoodHndlr AS varchar(300)) AS OBRK_NOFOODHNDLR_IND, -----21
			CAST(EPI_OutbreakUnidentified AS varchar(300)) AS OBRK_UNIDENTIFIED_IND, ----22
			CAST(EPI_OutbreakWaterborne AS varchar(300)) AS OBRK_WATERBORNE_IND, -----23
			CAST(EPI_RecDrugUse AS varchar(300)) AS STREET_DRUG_USED, ----24
			CAST(EPI_OutbreakAssoc AS varchar(300)) AS COM_SRC_OUTBREAK_IND ----25
			INTO dbo.TMP_D_INV_EPIDEMIOLOGY
			FROM dbo.TMP_F_INV_EPIDEMIOLOGY AS F
				 LEFT JOIN
				 [dbo].[D_INV_EPIDEMIOLOGY] AS D WITH(NOLOCK)
				 ON F.D_INV_EPIDEMIOLOGY_KEY = D.D_INV_EPIDEMIOLOGY_KEY
				 ORDER BY F.INVESTIGATION_KEY;

/*	  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET CHILDCARE_CASE_IND   = Case when CHILDCARE_CASE_IND is null then null  Else RTRIM(LTRIM(CHILDCARE_CASE_IND )) end ---1
																	  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET CNTRY_USUAL_RESIDENCE= Case when CNTRY_USUAL_RESIDENCE is null then null Else  RTRIM(LTRIM(CNTRY_USUAL_RESIDENCE)) end----2
																	  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET CT_BABYSITTER_IND    = Case when CT_BABYSITTER_IND  is null then null Else  RTRIM(LTRIM(CT_BABYSITTER_IND)) end----3
																	  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET CT_CHILDCARE_IND     = Case when CT_CHILDCARE_IND   is null then null Else  RTRIM(LTRIM(CT_CHILDCARE_IND)) end----4
																	  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET CT_HOUSEHOLD_IND     = Case when CT_HOUSEHOLD_IND   is null then null Else  RTRIM(LTRIM(CT_HOUSEHOLD_IND)) end----5
																	  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET HEP_CONTACT_IND      = Case when HEP_CONTACT_IND    is null then null Else  RTRIM(LTRIM(HEP_CONTACT_IND)) end----6
																	  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET OTHER_CONTACT_IND    = Case when OTHER_CONTACT_IND  is null then null Else  RTRIM(LTRIM(OTHER_CONTACT_IND))end----7
																	  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET CONTACT_TYPE_OTH     = Case when CONTACT_TYPE_OTH   is null then null Else  RTRIM(LTRIM(CONTACT_TYPE_OTH)) end----8
																	  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET CT_PLAYMATE_IND      = Case when CT_PLAYMATE_IND    is null then null Else  RTRIM(LTRIM(CT_PLAYMATE_IND))  end----9
																	  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET SEXUAL_PARTNER_IND   = Case when SEXUAL_PARTNER_IND is null then null Else  RTRIM(LTRIM(SEXUAL_PARTNER_IND)) end---10

										   
																				  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET DNP_HOUSEHOLD_CT_IND  = Case when DNP_HOUSEHOLD_CT_IND is null then null Else RTRIM(LTRIM(DNP_HOUSEHOLD_CT_IND))end---11
																				  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET HEP_A_EPLINK_IND      = Case when HEP_A_EPLINK_IND     is null then null Else RTRIM(LTRIM(HEP_A_EPLINK_IND)) end---12
																				  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET FEMALE_SEX_PRTNR_NBR  = Case when FEMALE_SEX_PRTNR_NBR is null then null Else RTRIM(LTRIM(FEMALE_SEX_PRTNR_NBR)) end---13
																				  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET FOODHNDLR_PRIOR_IND   = Case when FOODHNDLR_PRIOR_IND  is null then null Else RTRIM(LTRIM(FOODHNDLR_PRIOR_IND)) end---14
																				  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET DNP_EMPLOYEE_IND      = Case when DNP_EMPLOYEE_IND     is null then null Else RTRIM(LTRIM(DNP_EMPLOYEE_IND)) end---15
																				  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET STREET_DRUG_INJECTED  = Case when STREET_DRUG_INJECTED is null then null Else RTRIM(LTRIM(STREET_DRUG_INJECTED))end----16
																				  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET MALE_SEX_PRTNR_NBR    = Case when MALE_SEX_PRTNR_NBR   is null then null Else RTRIM(LTRIM(MALE_SEX_PRTNR_NBR))end---17
																				 ----- UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET OUTBREAK_IND          = Case when EPI_OutbreakAssoc     is null then '' Else TRIM(EPI_OutbreakAssoc)      end---18
																				  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET OBRK_FOODHNDLR_IND    = Case when OBRK_FOODHNDLR_IND   is null then  null Else RTRIM(LTRIM(OBRK_FOODHNDLR_IND)) end----19
																				  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET FOOD_OBRK_FOOD_ITEM   = Case when FOOD_OBRK_FOOD_ITEM  is null then null Else RTRIM(LTRIM(FOOD_OBRK_FOOD_ITEM)) end----20

										    
																				  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET OBRK_NOFOODHNDLR_IND = Case when OBRK_NOFOODHNDLR_IND  is null then null Else RTRIM(LTRIM(OBRK_NOFOODHNDLR_IND)) end----21
																				  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET OBRK_UNIDENTIFIED_IND= Case when OBRK_UNIDENTIFIED_IND is null then null Else RTRIM(LTRIM(OBRK_UNIDENTIFIED_IND))end---22
																				  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET OBRK_WATERBORNE_IND  = Case when OBRK_WATERBORNE_IND   is null then null Else RTRIM(LTRIM(OBRK_WATERBORNE_IND))end---23
																				  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET STREET_DRUG_USED     = Case when STREET_DRUG_USED      is null then null Else RTRIM(LTRIM(STREET_DRUG_USED)) end----24
																				  UPDATE dbo.TMP_D_INV_EPIDEMIOLOGY SET COM_SRC_OUTBREAK_IND = Case when COM_SRC_OUTBREAK_IND  is null then null Else RTRIM(LTRIM(COM_SRC_OUTBREAK_IND)) end----25
*/

			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;
			--------------------------------------------------------------------------------------15. CREATE TABLE dbo.D_INV_LAB_FINDING_TMP----------------------------
			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating  dbo.TMP_D_INV_LAB_FINDING';
			SET @Proc_Step_no = 15;
			IF OBJECT_ID('dbo.TMP_F_INV_LAB_FINDING', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_F_INV_LAB_FINDING;
			END;
			IF OBJECT_ID('dbo.TMP_D_INV_LAB_FINDING', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_INV_LAB_FINDING;
			END;
			SELECT F_PAGE_CASE.[D_INV_LAB_FINDING_KEY], F_PAGE_CASE.[INVESTIGATION_KEY]
			INTO dbo.TMP_F_INV_LAB_FINDING
			FROM dbo.F_PAGE_CASE WITH(NOLOCK)
				 INNER JOIN
				 TMP_F_PAGE_CASE AS PAGE_CASE
				 ON F_PAGE_CASE.INVESTIGATION_KEY = PAGE_CASE.INVESTIGATION_KEY---(my table)
				 ORDER BY D_INV_LAB_FINDING_KEY;
			SELECT F.[D_INV_LAB_FINDING_KEY], F.[INVESTIGATION_KEY] AS LAB_INV_KEY, CAST(LAB_TotalAntiHCV AS varchar(300)) AS HEP_C_TOTAL_ANTIBODY, ---1
			CAST(LAB_Supplem_antiHCV_Date AS date) AS SUPP_ANTI_HCV_DT, ------2
			CAST(LAB_ALT_Result AS varchar(300)) AS ALT_SGPT_RESULT, ---3
			CAST(LAB_AntiHBsPositive AS varchar(300)) AS ANTI_HBS_POS_REAC_IND, ---4
			CAST(LAB_AntiHBsTested AS varchar(300)) AS ANTI_HBSAG_TESTED_IND, ----5
			CAST(LAB_AST_Result AS varchar(300)) AS AST_SGOT_RESULT, ---6
			CAST(LAB_HBeAg AS varchar(300)) AS HEP_E_ANTIGEN, ---7
			CAST(LAB_HBeAg_Date AS date) AS HBE_AG_DT, ---8
			CAST(LAB_HBsAg AS varchar(300)) AS HEP_B_SURFACE_ANTIGEN, --9
			CAST(LAB_HBsAg_Date AS date) AS HBS_AG_DT, --10

			CAST(LAB_HBV_NAT AS varchar(300)) AS HEP_B_DNA, -----11
			CAST(LAB_HBV_NAT_Date AS date) AS HBV_NAT_DT, ---12
			CAST(LAB_HCVRNA AS varchar(300)) AS HCV_RNA, ---13
			CAST(LAB_HCVRNA_Date AS date) AS HCV_RNA_DT, ---14
			CAST(LAB_HepDTest AS varchar(300)) AS HEP_D_TEST_IND, -----15
			CAST(LAB_IgM_AntiHAV AS varchar(300)) AS HEP_A_IGM_ANTIBODY, ---16
			CAST(LAB_IgMAntiHAVDate AS date) AS IGM_ANTI_HAV_DT, ---17
			CAST(LAB_IgMAntiHBc AS varchar(300)) AS HEP_B_IGM_ANTIBODY, ---18
			CAST(LAB_IgMAntiHBcDate AS date) AS IGM_ANTI_HBC_DT, ---19
			CAST(LAB_PrevNegHepTest AS varchar(300)) AS PREV_NEG_HEP_TEST_IND, --20

			CAST(LAB_SignalToCutoff AS varchar(300)) AS ANTIHCV_SIGCUT_RATIO, ---21
			CAST(LAB_Supplem_antiHCV AS varchar(300)) AS ANTIHCV_SUPP_ASSAY, ---22
			CAST(LAB_TestDate AS date) AS ALT_RESULT_DT, -----23
			CAST(LAB_TestDate2 AS date) AS AST_RESULT_DT, ---24
			CAST(LAB_TestResultUpperLimit AS varchar(300)) AS ALT_SGPT_RSLT_UP_LMT, ---25
			CAST(LAB_TestResultUpperLimit2 AS varchar(300)) AS AST_SGOT_RSLT_UP_LMT, ---26
			CAST(LAB_TotalAntiHAV AS varchar(300)) AS HEP_A_TOTAL_ANTIBODY, ---27
			CAST(LAB_TotalAntiHAVDate AS date) AS TOTAL_ANTI_HAV_DT, ---28
			CAST(LAB_TotalAntiHBc AS varchar(300)) AS HEP_B_TOTAL_ANTIBODY, ---29
			CAST(LAB_TotalAntiHBcDate AS date) AS TOTAL_ANTI_HBC_DT, ----30

			CAST(LAB_TotalAntiHCV_Date AS date) AS TOTAL_ANTI_HCV_DT, --31
			CAST(LAB_TotalAntiHDV AS varchar(300)) AS HEP_D_TOTAL_ANTIBODY, ---32
			CAST(LAB_TotalAntiHDV_Date AS date) AS TOTAL_ANTI_HDV_DT, ---33
			CAST(LAB_TotalAntiHEV AS varchar(300)) AS HEP_E_TOTAL_ANTIBODY, ---34
			CAST(LAB_TotalAntiHEV_Date AS date) AS TOTAL_ANTI_HEV_DT, ---35
			CAST(LAB_VerifiedTestDate AS date) AS VERIFIED_TEST_DT ---36
			INTO dbo.TMP_D_INV_LAB_FINDING
			FROM dbo.TMP_F_INV_LAB_FINDING AS F
				 LEFT JOIN
				 [dbo].[D_INV_LAB_FINDING] AS D WITH(NOLOCK)
				 ON F.D_INV_LAB_FINDING_KEY = D.D_INV_LAB_FINDING_KEY
				 ORDER BY F.INVESTIGATION_KEY;

/*	UPDATE dbo.TMP_D_INV_LAB_FINDING SET HEP_C_TOTAL_ANTIBODY  = Case when  HEP_C_TOTAL_ANTIBODY is null then null Else RTRIM(LTRIM(HEP_C_TOTAL_ANTIBODY)) end;----1
																			UPDATE dbo.TMP_D_INV_LAB_FINDING SET ANTI_HBS_POS_REAC_IND = Case when  ANTI_HBS_POS_REAC_IND is null then null Else RTRIM(LTRIM(ANTI_HBS_POS_REAC_IND)) end;----4
																			UPDATE dbo.TMP_D_INV_LAB_FINDING SET ANTI_HBSAG_TESTED_IND = Case when  ANTI_HBSAG_TESTED_IND is null then null Else RTRIM(LTRIM(ANTI_HBSAG_TESTED_IND)) end;---5
																			UPDATE dbo.TMP_D_INV_LAB_FINDING SET HEP_E_ANTIGEN  = Case when  HEP_E_ANTIGEN  is null then null Else RTRIM(LTRIM(HEP_E_ANTIGEN)) end;----7
																			UPDATE dbo.TMP_D_INV_LAB_FINDING SET HEP_B_SURFACE_ANTIGEN = Case when HEP_B_SURFACE_ANTIGEN   is null then null Else RTRIM(LTRIM(HEP_B_SURFACE_ANTIGEN )) end;----9
																			----UPDATE dbo.TMP_D_INV_LAB_FINDING SET HBS_AG_DT = LAB_HBsAg_Date;-----------------------10
																			UPDATE dbo.TMP_D_INV_LAB_FINDING SET HEP_B_DNA   = Case when  HEP_B_DNA  is null then null Else RTRIM(LTRIM(HEP_B_DNA )) end;---11
																			UPDATE dbo.TMP_D_INV_LAB_FINDING SET HCV_RNA     = Case when  HCV_RNA    is null then null Else RTRIM(LTRIM(HCV_RNA)) end;------13
																			UPDATE dbo.TMP_D_INV_LAB_FINDING SET HEP_D_TEST_IND  = Case when  HEP_D_TEST_IND is null then null Else RTRIM(LTRIM(HEP_D_TEST_IND)) end;----15
																			UPDATE dbo.TMP_D_INV_LAB_FINDING SET HEP_A_IGM_ANTIBODY  = Case when HEP_A_IGM_ANTIBODY is null then null Else RTRIM(LTRIM(HEP_A_IGM_ANTIBODY)) end;---16
																			UPDATE dbo.TMP_D_INV_LAB_FINDING SET HEP_B_IGM_ANTIBODY  = Case when HEP_B_IGM_ANTIBODY is null then null Else RTRIM(LTRIM(HEP_B_IGM_ANTIBODY)) end;----18
																			UPDATE dbo.TMP_D_INV_LAB_FINDING SET PREV_NEG_HEP_TEST_IND= Case when  PREV_NEG_HEP_TEST_IND is null then null Else RTRIM(LTRIM(PREV_NEG_HEP_TEST_IND)) end;---20
																			UPDATE dbo.TMP_D_INV_LAB_FINDING SET ANTIHCV_SIGCUT_RATIO = Case when  ANTIHCV_SIGCUT_RATIO is null then null Else RTRIM(LTRIM(ANTIHCV_SIGCUT_RATIO)) end;----21
																			UPDATE dbo.TMP_D_INV_LAB_FINDING SET ANTIHCV_SUPP_ASSAY   = Case when  ANTIHCV_SUPP_ASSAY  is null then null Else RTRIM(LTRIM(ANTIHCV_SUPP_ASSAY )) end;----22
																			UPDATE dbo.TMP_D_INV_LAB_FINDING SET HEP_A_TOTAL_ANTIBODY  = Case when  HEP_A_TOTAL_ANTIBODY is null then null Else RTRIM(LTRIM(HEP_A_TOTAL_ANTIBODY)) end;----27
																			UPDATE dbo.TMP_D_INV_LAB_FINDING SET HEP_B_TOTAL_ANTIBODY = Case when HEP_B_TOTAL_ANTIBODY  is null then null Else RTRIM(LTRIM(HEP_B_TOTAL_ANTIBODY )) end;----29
																			UPDATE dbo.TMP_D_INV_LAB_FINDING SET HEP_D_TOTAL_ANTIBODY  = Case when  HEP_D_TOTAL_ANTIBODY is null then null Else RTRIM(LTRIM(HEP_D_TOTAL_ANTIBODY)) end;----32
																			UPDATE dbo.TMP_D_INV_LAB_FINDING SET HEP_E_TOTAL_ANTIBODY  = Case when  HEP_E_TOTAL_ANTIBODY is null then null Else RTRIM(LTRIM(HEP_E_TOTAL_ANTIBODY)) end;----34

																		*/

			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;
			---------------------------------------------------------------------------------------16. CREATE TABLE TMP_D_INV_MEDICAL_HISTORY----------------------------
			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating  dbo.TMP_D_INV_MEDICAL_HISTORY';
			SET @Proc_Step_no = 16;
			IF OBJECT_ID('dbo.TMP_F_INV_MEDICAL_HISTORY', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_F_INV_MEDICAL_HISTORY;
			END;
			IF OBJECT_ID('dbo.TMP_D_INV_MEDICAL_HISTORY', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_INV_MEDICAL_HISTORY;
			END;
			SELECT F_PAGE_CASE.D_INV_MEDICAL_HISTORY_KEY, F_PAGE_CASE.[INVESTIGATION_KEY]
			INTO TMP_F_INV_MEDICAL_HISTORY
			FROM dbo.F_PAGE_CASE WITH(NOLOCK)
				 INNER JOIN
				 TMP_F_PAGE_CASE AS PAGE_CASE
				 ON F_PAGE_CASE.INVESTIGATION_KEY = PAGE_CASE.INVESTIGATION_KEY---(my table)
				 ORDER BY D_INV_MEDICAL_HISTORY_KEY;
			SELECT F.D_INV_MEDICAL_HISTORY_KEY, F.INVESTIGATION_KEY AS MEDHistory_INV_KEY, CAST(MDH_DiabetesDxDate AS date) AS DIABETES_DX_DT, CAST(MDH_Diabetes AS [varchar](300)) AS DIABETES_IND, CAST(MDH_Jaundiced AS [varchar](300)) AS PAT_JUNDICED_IND, CAST(MDH_PrevAwareInfection AS [varchar](300)) AS PAT_PREV_AWARE_IND, CAST(MDH_ProviderOfCare AS [varchar](300)) AS HEP_CARE_PROVIDER, CAST(MDH_ReasonForTest AS [varchar](300)) AS TEST_REASON, CAST(MDH_ReasonForTestingOth AS [varchar](300)) AS TEST_REASON_OTH, CAST(MDH_Symptomatic AS [varchar](300)) AS SYMPTOMATIC_IND, CAST(MDH_DueDate AS date) AS PREGNANCY_DUE_DT
			INTO dbo.TMP_D_INV_MEDICAL_HISTORY
			FROM dbo.TMP_F_INV_MEDICAL_HISTORY AS F
				 LEFT JOIN
				 D_INV_MEDICAL_HISTORY AS D WITH(NOLOCK)
				 ON F.D_INV_MEDICAL_HISTORY_KEY = D.D_INV_MEDICAL_HISTORY_KEY
				 ORDER BY F.INVESTIGATION_KEY;

			--UPDATE dbo.TMP_D_INV_MEDICAL_HISTORY SET DIABETES_DX_DT = MDH_DiabetesDxDate	;----1
																		/*UPDATE dbo.TMP_D_INV_MEDICAL_HISTORY SET DIABETES_IND  = CASE WHEN DIABETES_IND is null then null else RTRIM(LTRIM(DIABETES_IND)) END;----2
																		UPDATE dbo.TMP_D_INV_MEDICAL_HISTORY SET PAT_JUNDICED_IND  = CASE WHEN PAT_JUNDICED_IND is null then null else RTRIM(LTRIM(PAT_JUNDICED_IND)) END;---3														 
																		UPDATE dbo.TMP_D_INV_MEDICAL_HISTORY SET PAT_PREV_AWARE_IND = CASE WHEN PAT_PREV_AWARE_IND is null then null else RTRIM(LTRIM(PAT_PREV_AWARE_IND)) END;---4														 
																		UPDATE dbo.TMP_D_INV_MEDICAL_HISTORY SET HEP_CARE_PROVIDER  = CASE WHEN HEP_CARE_PROVIDER  is null then null else RTRIM(LTRIM(HEP_CARE_PROVIDER)) END;---5
	
																		UPDATE dbo.TMP_D_INV_MEDICAL_HISTORY SET TEST_REASON = CASE WHEN TEST_REASON is null then Null else RTRIM(LTRIM(TEST_REASON)) END;---6
																		UPDATE dbo.TMP_D_INV_MEDICAL_HISTORY SET TEST_REASON_OTH = CASE WHEN TEST_REASON_OTH  is null then null else RTRIM(LTRIM( TEST_REASON_OTH)) END;---7
																		UPDATE dbo.TMP_D_INV_MEDICAL_HISTORY SET SYMPTOMATIC_IND = CASE WHEN SYMPTOMATIC_IND is null  then null else RTRIM(LTRIM(SYMPTOMATIC_IND )) END;---8
																		*/

			--UPDATE dbo.TMP_D_INV_MEDICAL_HISTORY SET PREGNANCY_DUE_DT=MDH_DueDate

			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;	
			---------------------------------------------------------------------------------17. CREATE TABLE TMP.D_INV_MOTHER-------------------------------------------------------------------
			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating  dbo.TMP_D_INV_MOTHER';
			SET @Proc_Step_no = 17;
			IF OBJECT_ID('dbo.TMP_F_INV_MOTHER', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_F_INV_MOTHER;
			END;
			IF OBJECT_ID('dbo.TMP_D_INV_MOTHER', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_INV_MOTHER;
			END;
			SELECT F_PAGE_CASE.D_INV_MOTHER_KEY, F_PAGE_CASE.INVESTIGATION_KEY
			INTO dbo.TMP_F_INV_MOTHER
			FROM dbo.F_PAGE_CASE WITH(NOLOCK)
				 INNER JOIN
				 TMP_F_PAGE_CASE AS PAGE_CASE
				 ON F_PAGE_CASE.INVESTIGATION_KEY = PAGE_CASE.INVESTIGATION_KEY---(my table)
				 ORDER BY D_INV_MOTHER_KEY;

				---changed it like this on 10/1/2021

				declare @SQL1 varchar(4000)
				set @SQL1='SELECT F.D_INV_MOTHER_KEY, F.INVESTIGATION_KEY AS MOTHER_INV_KEY, 
				CAST(MTH_MotherBornOutsideUS AS varchar(300)) AS MTH_BORN_OUTSIDE_US, 
				CAST(MTH_MotherEthnicity AS varchar(300)) AS MTH_ETHNICITY, 
				CAST(MTH_MotherHBsAgPosPrior AS varchar(300)) AS MTH_HBS_AG_PRIOR_POS, 
				CAST(MTH_MotherPositiveAfter AS varchar(300)) AS MTH_POS_AFTER, 
				CAST(MTH_MotherRace AS varchar(300)) AS MTH_RACE, CAST(MTH_MothersBirthCountry AS varchar(300)) AS MTH_BIRTH_COUNTRY, 
				CAST(MTH_MotherPosTestDate AS date) AS MTH_POS_TEST_DT
			INTO dbo.TMP_D_INV_MOTHER
			FROM dbo.TMP_F_INV_MOTHER AS F
				 LEFT JOIN
				 [dbo].[D_INV_MOTHER] AS D WITH(NOLOCK)
				 ON F.D_INV_MOTHER_KEY = D.D_INV_MOTHER_KEY
				 ORDER BY F.INVESTIGATION_KEY';
				 
				  exec (@SQL1);
			/*UPDATE dbo.TMP_D_INV_MOTHER
			  SET MTH_BORN_OUTSIDE_US = CASE
										WHEN MTH_BORN_OUTSIDE_US IS NULL THEN NULL
										ELSE RTRIM(LTRIM(MTH_BORN_OUTSIDE_US))
										END;
			UPDATE dbo.TMP_D_INV_MOTHER
			  SET MTH_ETHNICITY = CASE
								  WHEN MTH_ETHNICITY IS NULL THEN NULL
								  ELSE RTRIM(LTRIM(MTH_ETHNICITY))
								  END;
			UPDATE dbo.TMP_D_INV_MOTHER
			  SET MTH_HBS_AG_PRIOR_POS = CASE
										 WHEN MTH_HBS_AG_PRIOR_POS IS NULL THEN NULL
										 ELSE RTRIM(LTRIM(MTH_HBS_AG_PRIOR_POS))
										 END;
			UPDATE dbo.TMP_D_INV_MOTHER
			  SET MTH_POS_AFTER = CASE
								  WHEN MTH_POS_AFTER IS NULL THEN NULL
								  ELSE RTRIM(LTRIM(MTH_POS_AFTER))
								  END;
			UPDATE dbo.TMP_D_INV_MOTHER
			  SET MTH_RACE = CASE
							 WHEN MTH_RACE IS NULL THEN NULL
							 ELSE RTRIM(LTRIM(MTH_RACE))
							 END;
			UPDATE dbo.TMP_D_INV_MOTHER
			  SET MTH_BIRTH_COUNTRY = CASE
									  WHEN MTH_BIRTH_COUNTRY IS NULL THEN NULL
									  ELSE RTRIM(LTRIM(MTH_BIRTH_COUNTRY))
									  END;
									  */
			--UPDATE dbo.TMP_D_INV_MOTHER SET MTH_POS_TEST_DT   = MTH_POS_TEST_DT ;

			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;
			----------------------------------------------------------------------------------18. CREATE TABLE TMP_D_INV_RISK_FACTOR---------------------------
			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating  dbo.TMP_D_INV_RISK_FACTOR';
			SET @Proc_Step_no = 18;
			IF OBJECT_ID('dbo.TMP_F_INV_RISK_FACTOR', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_F_INV_RISK_FACTOR;
			END;
			IF OBJECT_ID('dbo.TMP_D_INV_RISK_FACTOR', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_INV_RISK_FACTOR;
			END;
			SELECT F_PAGE_CASE.D_INV_RISK_FACTOR_KEY, F_PAGE_CASE.INVESTIGATION_KEY
			INTO dbo.TMP_F_INV_RISK_FACTOR
			FROM dbo.F_PAGE_CASE WITH(NOLOCK)
				 INNER JOIN
				 TMP_F_PAGE_CASE AS PAGE_CASE
				 ON F_PAGE_CASE.INVESTIGATION_KEY = PAGE_CASE.INVESTIGATION_KEY	---(my table)
				 ORDER BY D_INV_RISK_FACTOR_KEY;
			SELECT F.D_INV_RISK_FACTOR_KEY, F.INVESTIGATION_KEY AS RISK_INV_KEY, CAST(RSK_BloodExpOther AS varchar(300)) AS BLD_EXPOSURE_IND, ---1
			CAST(RSK_BloodTransfusion AS varchar(300)) AS BLD_RECVD_IND, ----2
			CAST(RSK_BloodTransfusionDate AS date) AS BLD_RECVD_DT, ---3
			CAST(RSK_BloodWorkerCnctFreq AS varchar(300)) AS MED_DEN_BLD_CT_FRQ, ---4
			CAST(RSK_BloodWorkerEver AS varchar(300)) AS MED_DEN_EMP_EVER_IND, ---5
			CAST(RSK_BloodWorkerOnset AS varchar(300)) AS MED_DEN_EMPLOYEE_IND, ---6

			CAST(RSK_ClottingPrior87 AS varchar(300)) AS CLOTFACTOR_PRIOR_1987, ----7
			CAST(RSK_ContaminatedStick AS varchar(300)) AS BLD_CONTAM_IND, ----8
			CAST(RSK_DentalOralSx AS varchar(300)) AS DEN_WORK_OR_SURG_IND, ----9
			CAST(RSK_HEMODIALYSIS_BEFORE_ONSET AS varchar(300)) AS HEMODIALYSIS_IND, --10

			CAST(RSK_HemodialysisLongTerm AS varchar(300)) AS LT_HEMODIALYSIS_IND, --11
			CAST(RSK_HospitalizedPrior AS varchar(300)) AS HSPTL_PRIOR_ONSET_IND, --12
			CAST(RSK_IDU AS varchar(300)) AS EVER_INJCT_NOPRSC_DRG, ------13
			CAST(RSK_Incarcerated24Hrs AS varchar(300)) AS INCAR_24PLUSHRS_IND, ---14
			CAST(RSK_Incarcerated6months AS varchar(300)) AS INCAR_6PLUS_MO_IND, ----15
			CAST(RSK_IncarceratedEver AS varchar(300)) AS EVER_INCAR_IND, ---16
			CAST(RSK_IncarceratedJail AS varchar(300)) AS INCAR_TYPE_JAIL_IND, -- -17
			CAST(RSK_IncarcerationPrison AS varchar(300)) AS INCAR_TYPE_PRISON_IND, ---18
			CAST(RSK_IncarcJuvenileFacilit AS varchar(300)) AS INCAR_TYPE_JUV_IND, ---19
			CAST(RSK_IncarcTimeMonths AS varchar(300)) AS LAST6PLUSMO_INCAR_PER, ------20

			CAST(RSK_IncarcYear6Mos AS varchar(300)) AS LAST6PLUSMO_INCAR_YR, ---21
			CAST(RSK_IVInjectInfuseOutpt AS varchar(300)) AS OUTPAT_IV_INF_IND, ---22
			CAST(RSK_LongTermCareRes AS varchar(300)) AS LTCARE_RESIDENT_IND, ---23
			CAST(RSK_NumSexPrtners AS varchar(300)) AS LIFE_SEX_PRTNR_NBR, ---24
			CAST(RSK_OtherBldExpSpec AS varchar(300)) AS BLD_EXPOSURE_OTH, ---25
			CAST(RSK_Piercing AS varchar(300)) AS PIERC_PRIOR_ONSET_IND, ---26

			CAST(RSK_PiercingOthLocSpec AS varchar(300)) AS PIERC_PERF_LOC_OTH, ----27                                                                                                            
			CAST(RSK_PiercingRcvdFrom AS varchar(300)) AS PIERC_PERF_LOC, -------28
			CAST(RSK_PSWrkrBldCnctFreq AS varchar(300)) AS PUB_SAFETY_BLD_CT_FRQ, ---29
			CAST(RSK_PublicSafetyWorker AS varchar(300)) AS PUB_SAFETY_WORKER_IND, ---30

			CAST(RSK_STDTxEver AS varchar(300)) AS STD_TREATED_IND, ---31
			CAST(RSK_STDTxYr AS varchar(300)) AS STD_LAST_TREATMENT_YR, ---32
			CAST(RSK_SurgeryOther AS varchar(300)) AS NON_ORAL_SURGERY_IND, -----33
			CAST(RSK_Tattoo AS varchar(300)) AS TATT_PRIOR_ONSET_IND, ----34
			CAST(RSK_TattooLocation AS varchar(300)) AS TATTOO_PERF_LOC, ----35
			CAST(RSK_TattooLocOthSpec AS varchar(300)) AS TATT_PRIOR_LOC_OTH, ---36
			CAST(RSK_TransfusionPrior92 AS varchar(300)) AS BLD_TRANSF_PRIOR_1992, ---37
			CAST(RSK_TransplantPrior92 AS varchar(300)) AS ORGN_TRNSP_PRIOR_1992, ---38
			CAST(RSK_HepContactEver AS varchar(300)) AS HEP_CONTACT_EVER_IND     ----39
			INTO dbo.TMP_D_INV_RISK_FACTOR
			FROM dbo.TMP_F_INV_RISK_FACTOR AS F
				 LEFT JOIN
				 [dbo].[D_INV_RISK_FACTOR] AS D WITH(NOLOCK)
				 ON F.D_INV_RISK_FACTOR_KEY = D.D_INV_RISK_FACTOR_KEY
				 ORDER BY RISK_INV_KEY;

			---UPDATE dbo.TMP_D_INV_RISK_FACTOR	SET RISK_INV_KEY           = CASE WHEN RISK_INV_KEY is null   then null  else RISK_INV_KEY   END;-----1
			/*UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET BLD_EXPOSURE_IND = CASE
									 WHEN BLD_EXPOSURE_IND IS NULL THEN NULL
									 ELSE RTRIM(LTRIM(BLD_EXPOSURE_IND))
									 END;----1
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET BLD_RECVD_IND = CASE
								  WHEN BLD_RECVD_IND IS NULL THEN NULL
								  ELSE RTRIM(LTRIM(BLD_RECVD_IND))
								  END;---2
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET BLD_RECVD_DT = BLD_RECVD_DT;----------3
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET MED_DEN_BLD_CT_FRQ = MED_DEN_BLD_CT_FRQ; 	--------4		
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET MED_DEN_EMP_EVER_IND = CASE
										 WHEN MED_DEN_EMP_EVER_IND IS NULL THEN NULL
										 ELSE RTRIM(LTRIM(MED_DEN_EMP_EVER_IND))
										 END;-------5
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET MED_DEN_EMPLOYEE_IND = CASE
										 WHEN MED_DEN_EMPLOYEE_IND IS NULL THEN NULL
										 ELSE RTRIM(LTRIM(MED_DEN_EMPLOYEE_IND))
										 END;-------6
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET CLOTFACTOR_PRIOR_1987 = CASE
										  WHEN CLOTFACTOR_PRIOR_1987 IS NULL THEN NULL
										  ELSE RTRIM(LTRIM(CLOTFACTOR_PRIOR_1987))
										  END;-----7
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET BLD_CONTAM_IND = CASE
								   WHEN BLD_CONTAM_IND IS NULL THEN NULL
								   ELSE RTRIM(LTRIM(BLD_CONTAM_IND))
								   END;---9
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET DEN_WORK_OR_SURG_IND = CASE
										 WHEN DEN_WORK_OR_SURG_IND IS NULL THEN NULL
										 ELSE RTRIM(LTRIM(DEN_WORK_OR_SURG_IND))
										 END;-----9
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET HEMODIALYSIS_IND = CASE
									 WHEN HEMODIALYSIS_IND IS NULL THEN NULL
									 ELSE RTRIM(LTRIM(HEMODIALYSIS_IND))
									 END;----10

			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET LT_HEMODIALYSIS_IND = CASE
										WHEN LT_HEMODIALYSIS_IND IS NULL THEN NULL
										ELSE RTRIM(LTRIM(LT_HEMODIALYSIS_IND))
										END;---11
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET HSPTL_PRIOR_ONSET_IND = CASE
										  WHEN HSPTL_PRIOR_ONSET_IND IS NULL THEN NULL
										  ELSE RTRIM(LTRIM(HSPTL_PRIOR_ONSET_IND))
										  END;----12
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET EVER_INJCT_NOPRSC_DRG = CASE
										  WHEN EVER_INJCT_NOPRSC_DRG IS NULL THEN NULL
										  ELSE RTRIM(LTRIM(EVER_INJCT_NOPRSC_DRG))
										  END;---------------13
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET INCAR_24PLUSHRS_IND = CASE
										WHEN INCAR_24PLUSHRS_IND IS NULL THEN NULL
										ELSE RTRIM(LTRIM(INCAR_24PLUSHRS_IND))
										END;-----14
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET INCAR_6PLUS_MO_IND = CASE
									   WHEN INCAR_6PLUS_MO_IND IS NULL THEN NULL
									   ELSE RTRIM(LTRIM(INCAR_6PLUS_MO_IND))
									   END;-----15
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET EVER_INCAR_IND = CASE
								   WHEN EVER_INCAR_IND IS NULL THEN NULL
								   ELSE RTRIM(LTRIM(EVER_INCAR_IND))
								   END;---16
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET INCAR_TYPE_JAIL_IND = CASE
										WHEN INCAR_TYPE_JAIL_IND IS NULL THEN NULL
										ELSE RTRIM(LTRIM(INCAR_TYPE_JAIL_IND))
										END;--------17
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET INCAR_TYPE_PRISON_IND = CASE
										  WHEN INCAR_TYPE_PRISON_IND IS NULL THEN NULL
										  ELSE RTRIM(LTRIM(INCAR_TYPE_PRISON_IND))
										  END;-----18
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET INCAR_TYPE_JUV_IND = CASE
									   WHEN INCAR_TYPE_JUV_IND IS NULL THEN NULL
									   ELSE RTRIM(LTRIM(INCAR_TYPE_JUV_IND))
									   END;-------19
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET LAST6PLUSMO_INCAR_PER = LAST6PLUSMO_INCAR_PER;------------  /*EXT_LAST6PLUSMO_INCAR_PER= INPUT(RSK_IncarcTimeMonths, comma20.)*/----20

			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET LAST6PLUSMO_INCAR_YR = LAST6PLUSMO_INCAR_YR;----------	/*EXT_LAST6PLUSMO_INCAR_YR= INPUT(RSK_IncarcYear6Mos, comma20.);*/-----21
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET OUTPAT_IV_INF_IND = CASE
									  WHEN OUTPAT_IV_INF_IND IS NULL THEN NULL
									  ELSE RTRIM(LTRIM(OUTPAT_IV_INF_IND))
									  END;---22
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET LTCARE_RESIDENT_IND = CASE
										WHEN LTCARE_RESIDENT_IND IS NULL THEN NULL
										ELSE RTRIM(LTRIM(LTCARE_RESIDENT_IND))
										END;---23
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET LIFE_SEX_PRTNR_NBR = LIFE_SEX_PRTNR_NBR;             ---------------24---INPUT
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET BLD_EXPOSURE_OTH = CASE
									 WHEN BLD_EXPOSURE_OTH IS NULL THEN NULL
									 ELSE RTRIM(LTRIM(BLD_EXPOSURE_OTH))
									 END;-----25
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET PIERC_PRIOR_ONSET_IND = CASE
										  WHEN PIERC_PRIOR_ONSET_IND IS NULL THEN NULL
										  ELSE RTRIM(LTRIM(PIERC_PRIOR_ONSET_IND))
										  END;           --26
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET PIERC_PERF_LOC_OTH = CASE
									   WHEN PIERC_PERF_LOC_OTH IS NULL THEN NULL
									   ELSE RTRIM(LTRIM(PIERC_PERF_LOC_OTH))
									   END;  -----27
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET PIERC_PERF_LOC = CASE
								   WHEN PIERC_PERF_LOC IS NULL THEN NULL
								   ELSE RTRIM(LTRIM(PIERC_PERF_LOC))
								   END;--28
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET PUB_SAFETY_BLD_CT_FRQ = CASE
										  WHEN PUB_SAFETY_BLD_CT_FRQ IS NULL THEN NULL
										  ELSE RTRIM(LTRIM(PUB_SAFETY_BLD_CT_FRQ))
										  END;--29
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET PUB_SAFETY_WORKER_IND = CASE
										  WHEN PUB_SAFETY_WORKER_IND IS NULL THEN NULL
										  ELSE RTRIM(LTRIM(PUB_SAFETY_WORKER_IND))
										  END;---30

			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET STD_TREATED_IND = CASE
									WHEN STD_TREATED_IND IS NULL THEN NULL
									ELSE RTRIM(LTRIM(STD_TREATED_IND))
									END;----31
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET STD_LAST_TREATMENT_YR = STD_LAST_TREATMENT_YR;            --------------32----input
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET NON_ORAL_SURGERY_IND = CASE
										 WHEN NON_ORAL_SURGERY_IND IS NULL THEN NULL
										 ELSE RTRIM(LTRIM(NON_ORAL_SURGERY_IND))
										 END;------33
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET TATT_PRIOR_ONSET_IND = CASE
										 WHEN TATT_PRIOR_ONSET_IND IS NULL THEN NULL
										 ELSE RTRIM(LTRIM(TATT_PRIOR_ONSET_IND))
										 END;-------34
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET TATTOO_PERF_LOC = CASE
									WHEN TATTOO_PERF_LOC IS NULL THEN NULL
									ELSE RTRIM(LTRIM(TATTOO_PERF_LOC))
									END;---35
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET TATT_PRIOR_LOC_OTH = CASE
									   WHEN TATT_PRIOR_LOC_OTH IS NULL THEN NULL
									   ELSE RTRIM(LTRIM(TATT_PRIOR_LOC_OTH))
									   END;-----36
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET BLD_TRANSF_PRIOR_1992 = CASE
										  WHEN BLD_TRANSF_PRIOR_1992 IS NULL THEN NULL
										  ELSE RTRIM(LTRIM(BLD_TRANSF_PRIOR_1992))
										  END;----37
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET ORGN_TRNSP_PRIOR_1992 = CASE
										  WHEN ORGN_TRNSP_PRIOR_1992 IS NULL THEN NULL
										  ELSE RTRIM(LTRIM(ORGN_TRNSP_PRIOR_1992))
										  END;---38
			UPDATE dbo.TMP_D_INV_RISK_FACTOR
			  SET HEP_CONTACT_EVER_IND = CASE
										 WHEN HEP_CONTACT_EVER_IND IS NULL THEN NULL
										 ELSE RTRIM(LTRIM(HEP_CONTACT_EVER_IND))
										 END;---39
										 */
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;
			---------------------------------------------------19. CREATE TABLE dbo.TMP_D_INV_TRAVEL---------------------------

			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating  dbo.TMP_D_INV_TRAVEL';
			SET @Proc_Step_no = 19;
			IF OBJECT_ID('dbo.TMP_F_INV_TRAVEL', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_F_INV_TRAVEL;
			END;
			IF OBJECT_ID('dbo.TMP_D_INV_TRAVEL', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_INV_TRAVEL;
			END;
			SELECT F_PAGE_CASE.D_INV_TRAVEL_KEY, F_PAGE_CASE.INVESTIGATION_KEY
			INTO dbo.TMP_F_INV_TRAVEL
			FROM dbo.F_PAGE_CASE WITH(NOLOCK)
				 INNER JOIN
				 dbo.TMP_F_PAGE_CASE AS PAGE_CASE
				 ON F_PAGE_CASE.INVESTIGATION_KEY = PAGE_CASE.INVESTIGATION_KEY---(my table)
				 ORDER BY D_INV_TRAVEL_KEY;
			SELECT F.D_INV_TRAVEL_KEY, F.INVESTIGATION_KEY AS Travel_INV_KEY, CAST(TRV_HouseholdTravel AS varchar(300)) AS HOUSEHOLD_TRAVEL_IND, CAST(TRV_PatientTravel AS varchar(300)) AS TRAVEL_OUT_USACAN_IND, CAST(TRV_PtTravelCountries AS varchar(300)) AS TRAVEL_OUT_USACAN_LOC, CAST(TRV_TravelCountryHouse AS varchar(300)) AS HOUSEHOLD_TRAVEL_LOC, CAST(TRV_VHF_TRAVEL_REASON AS varchar(300)) AS TRAVEL_REASON
			INTO dbo.TMP_D_INV_TRAVEL
			FROM dbo.TMP_F_INV_TRAVEL AS F
				 LEFT JOIN
				 [dbo].[D_INV_TRAVEL] AS D WITH(NOLOCK)
				 ON F.D_INV_TRAVEL_KEY = D.D_INV_TRAVEL_KEY
				 ORDER BY F.INVESTIGATION_KEY;
			/*UPDATE dbo.TMP_D_INV_TRAVEL
			  SET HOUSEHOLD_TRAVEL_IND = CASE
										 WHEN HOUSEHOLD_TRAVEL_IND IS NULL THEN NULL
										 ELSE RTRIM(LTRIM(HOUSEHOLD_TRAVEL_IND))
										 END;----1
			UPDATE dbo.TMP_D_INV_TRAVEL
			  SET TRAVEL_OUT_USACAN_IND = CASE
										  WHEN TRAVEL_OUT_USACAN_IND IS NULL THEN NULL
										  ELSE RTRIM(LTRIM(TRAVEL_OUT_USACAN_IND))
										  END;----2
			UPDATE dbo.TMP_D_INV_TRAVEL
			  SET TRAVEL_OUT_USACAN_LOC = CASE
										  WHEN TRAVEL_OUT_USACAN_LOC IS NULL THEN NULL
										  ELSE RTRIM(LTRIM(TRAVEL_OUT_USACAN_LOC))
										  END;----3
			UPDATE dbo.TMP_D_INV_TRAVEL
			  SET HOUSEHOLD_TRAVEL_LOC = CASE
										 WHEN HOUSEHOLD_TRAVEL_LOC IS NULL THEN NULL
										 ELSE RTRIM(LTRIM(HOUSEHOLD_TRAVEL_LOC))
										 END;----4
			UPDATE dbo.TMP_D_INV_TRAVEL
			  SET TRAVEL_REASON = CASE
								  WHEN TRAVEL_REASON IS NULL THEN NULL
								  ELSE RTRIM(LTRIM(TRAVEL_REASON))
								  END;----5
			*/
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;	
			-------------------------------------------------------------------20. CREATE TABLE TMP_D_INV_VACCINATION--------------------------
			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating  TMP_D_INV_VACCINATION';
			SET @Proc_Step_no = 20;
			IF OBJECT_ID('dbo.TMP_F_INV_VACCINATION', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_F_INV_VACCINATION;
			END;
			IF OBJECT_ID('dbo.TMP_D_INV_VACCINATION', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_INV_VACCINATION;
			END;
			SELECT F_PAGE_CASE.D_INV_VACCINATION_KEY, F_PAGE_CASE.INVESTIGATION_KEY
			INTO dbo.TMP_F_INV_VACCINATION
			FROM dbo.F_PAGE_CASE WITH(NOLOCK)
				 INNER JOIN
				 TMP_F_PAGE_CASE AS PAGE_CASE
				 ON F_PAGE_CASE.INVESTIGATION_KEY = PAGE_CASE.INVESTIGATION_KEY	---(my table)
				 ORDER BY D_INV_VACCINATION_KEY;

/*SELECT
																					   D_INV_VACCINATION_KEY,
																					   INVESTIGATION_KEY ,
																					  ---- EXT_VACC_RECVD_DT as VACC_RECVD_DT, 
																					   EXT_IMM_GLOB_RECVD_IND as IMM_GLOB_RECVD_IND,--------[VAC_ImmuneGlobulin]
																					   EXT_GLOB_LAST_RECVD_YR as GLOB_LAST_RECVD_YR, -----[VAC_LastIGDose]
																					   EXT_VACC_RECVD_IND as VACC_RECVD_IND,---------[VAC_Vacc_Rcvd]
																					   EXT_VACC_DOSE_RECVD_NBR as VACC_DOSE_RECVD_NBR,------ [VAC_VaccineDoses]
																					   EXT_VACC_LAST_RECVD_YR as VACC_LAST_RECVD_YR-----[VAC_YearofLastDose]*/

			SELECT F.D_INV_VACCINATION_KEY, F.INVESTIGATION_KEY AS VACCINATION_INV_KEY, CAST(VAC_ImmuneGlobulin AS varchar(300)) AS IMM_GLOB_RECVD_IND, ----EXT_IMM_GLOB_RECVD_IND,
			CAST(VAC_LastIGDose AS varchar(300)) AS GLOB_LAST_RECVD_YR, -----EXT_GLOB_LAST_RECVD_YR,
			CAST(VAC_Vacc_Rcvd AS varchar(10)) AS VACC_RECVD_IND, ---- ---EXT_VACC_RECVD_IND--------------5-13-2021
			CAST(VAC_VaccineDoses AS varchar(300)) AS VACC_DOSE_RECVD_NBR, ---EXT_VACC_DOSE_RECVD_NBR,-----INPUT(VAC_VaccineDoses, comma20.);                                       
			CAST(VAC_YearofLastDose AS varchar(300)) AS VACC_LAST_RECVD_YR -----EXT_VACC_LAST_RECVD_YR,-------INPUT(VAC_YearofLastDose, comma20.); 
			--	Cast(VAC_VaccinationDate  as varchar(2000)) as VAC_VaccinationDate  ---EXT_VACC_RECVD_DT,------------VAC_VaccinationDate---could not find
			INTO dbo.TMP_D_INV_VACCINATION
			FROM dbo.TMP_F_INV_VACCINATION AS F
				 LEFT JOIN
				 [dbo].[D_INV_VACCINATION] AS V WITH(NOLOCK)
				 ON F.D_INV_VACCINATION_KEY = V.D_INV_VACCINATION_KEY
				 ORDER BY F.INVESTIGATION_KEY;
			/*UPDATE dbo.TMP_D_INV_VACCINATION
			  SET IMM_GLOB_RECVD_IND = CASE
									   WHEN IMM_GLOB_RECVD_IND IS NULL THEN NULL
									   ELSE IMM_GLOB_RECVD_IND
									   END;---EXT_IMM_GLOB_RECVD_IND=IMM_GLOB_RECVD_IND
			UPDATE dbo.TMP_D_INV_VACCINATION
			  SET VACC_RECVD_IND = CASE
								   WHEN VACC_RECVD_IND IS NULL THEN NULL
								   ELSE RTRIM(LTRIM(VACC_RECVD_IND))
								   END;*/

/*IF missing(RSK_STDTxYr) then do; EXT_STD_LAST_TREATMENT_YR=''; end;
																					else do; 
																					EXT_STD_LAST_TREATMENT_YR=trim(RSK_STDTxYr); end;---------? No field in Vaccination*/
/*IF missing(VAC_VaccinationDate) then do; EXT_VACC_RECVD_DT=''; end;
																					   else do; 
																					EXT_VACC_RECVD_DT=trim(VAC_VaccinationDate); end;*/

			--------Could Not find

			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;
			----------------------------------------------------------21. CREATE TABLE dbo.TMP_D_Patient---------------------------
			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating  dbo.TMP_D_Patient';
			SET @Proc_Step_no = 21;
			IF OBJECT_ID('dbo.TMP_D_Patient', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_Patient;
			END;
			SELECT F_PAGE_CASE.INVESTIGATION_KEY AS Patient_INV_KEY, D_PATIENT.PATIENT_UID, D_PATIENT.PATIENT_ETHNICITY AS PAT_ETHNICITY, D_PATIENT.PATIENT_AGE_REPORTED AS PAT_REPORTED_AGE, D_PATIENT.PATIENT_AGE_REPORTED_UNIT AS PAT_REPORTED_AGE_UNIT, D_PATIENT.PATIENT_CITY AS PAT_CITY, D_PATIENT.PATIENT_COUNTRY AS PAT_COUNTRY, D_PATIENT.PATIENT_BIRTH_COUNTRY AS PAT_BIRTH_COUNTRY, D_PATIENT.PATIENT_COUNTY AS PAT_COUNTY, D_PATIENT.PATIENT_CURRENT_SEX AS PAT_CURR_GENDER, D_PATIENT.PATIENT_DOB AS PAT_DOB, D_PATIENT.PATIENT_FIRST_NAME AS PAT_FIRST_NM, D_PATIENT.PATIENT_LAST_NAME AS PAT_LAST_NM, SUBSTRING(LTRIM(RTRIM(D_PATIENT.PATIENT_LOCAL_ID)), 1, 25) AS PAT_LOCAL_ID, ---added 9/9/2021		
			D_PATIENT.PATIENT_MIDDLE_NAME AS PAT_MIDDLE_NM, D_PATIENT.PATIENT_RACE_CALCULATED AS PAT_RACE, D_PATIENT.PATIENT_STATE AS PAT_STATE, D_PATIENT.PATIENT_STREET_ADDRESS_1 AS PAT_STREET_ADDR_1, D_PATIENT.PATIENT_STREET_ADDRESS_2 AS PAT_STREET_ADDR_2, SUBSTRING(LTRIM(RTRIM(D_PATIENT.PATIENT_ZIP)), 1, 10) AS PAT_ZIP_CODE, ---added 9/9/2021
			D_PATIENT.PATIENT_ENTRY_METHOD AS PAT_ELECTRONIC_IND, D_PATIENT.PATIENT_ADD_TIME AS INV_ADD_TIME
			INTO dbo.TMP_D_Patient
			----FROM F_PAGE_CASE
			FROM dbo.TMP_F_PAGE_CASE AS F_PAGE_CASE---(my table)
				 INNER JOIN
				 TMP_CONDITION AS T
				 ON F_PAGE_CASE.CONDITION_KEY = T.CONDITION_KEY---(my table)
				 INNER JOIN
				 dbo.D_PATIENT WITH(NOLOCK)
				 ON D_PATIENT.PATIENT_KEY = F_PAGE_CASE.PATIENT_KEY
				 ORDER BY INVESTIGATION_KEY;
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;
			---------------------------------------------------22. CREATE TABLE TMP_INVESTIGATION---------------------------
			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating  dbo.TMP_Investigation';
			SET @Proc_Step_no = 22;
			IF OBJECT_ID('dbo.TMP_Investigation', 'U') IS NOT NULL
			BEGIN
				DROP TABLE TMP_Investigation;
			END;
			SELECT PAGE_CASE.INVESTIGATION_KEY, INVESTIGATION.CASE_UID, T.CONDITION_CD AS CONDITION_CD, INVESTIGATION.CASE_OID AS PROGRAM_JURISDICTION_OID, CAST([CASE_RPT_MMWR_WK] AS varchar(100)) AS CASE_RPT_MMWR_WEEK, CAST([CASE_RPT_MMWR_YR] AS varchar(100)) AS CASE_RPT_MMWR_YEAR, INVESTIGATION.DIAGNOSIS_DT AS DIAGNOSIS_DT, INVESTIGATION.DIE_FRM_THIS_ILLNESS_IND AS DIE_FRM_THIS_ILLNESS_IND, INVESTIGATION.DISEASE_IMPORTED_IND AS DISEASE_IMPORTED_IND, INVESTIGATION.EARLIEST_RPT_TO_CNTY_DT AS EARLIEST_RPT_TO_CNTY, INVESTIGATION.EARLIEST_RPT_TO_STATE_DT AS EARLIEST_RPT_TO_STATE_DT, INVESTIGATION.HSPTL_ADMISSION_DT AS HSPTL_ADMISSION_DT, INVESTIGATION.HSPTL_DISCHARGE_DT AS HSPTL_DISCHARGE_DT, INVESTIGATION.HSPTL_DURATION_DAYS AS HSPTL_DURATION_DAYS, INVESTIGATION.HSPTLIZD_IND AS HSPTLIZD_IND, INVESTIGATION.ILLNESS_ONSET_DT AS ILLNESS_ONSET_DT, INVESTIGATION.IMPORT_FRM_CITY AS IMPORT_FROM_CITY, INVESTIGATION.IMPORT_FRM_CNTRY AS IMPORT_FROM_COUNTRY, INVESTIGATION.IMPORT_FRM_CNTY AS IMPORT_FROM_COUNTY, INVESTIGATION.IMPORT_FRM_STATE AS IMPORT_FROM_STATE, INVESTIGATION.INV_CASE_STATUS AS INV_CASE_STATUS, LTRIM(RTRIM(INVESTIGATION.INV_COMMENTS)) AS INV_COMMENTS, ---added 7/21/2021	
			INVESTIGATION.INV_LOCAL_ID AS INV_LOCAL_ID, INVESTIGATION.INV_RPT_DT AS INV_RPT_DT, INVESTIGATION.INV_START_DT AS INV_START_DT,		
			----	INVESTIGATION.INVESTIGATION_KEY	AS 	INVESTIGATION_KEY,		
			INVESTIGATION.INVESTIGATION_STATUS AS INVESTIGATION_STATUS, INVESTIGATION.JURISDICTION_NM AS JURISDICTION_NM, INVESTIGATION.OUTBREAK_IND AS OUTBREAK_IND, INVESTIGATION.PATIENT_PREGNANT_IND AS PAT_PREGNANT_IND, INVESTIGATION.RPT_SRC_CD_DESC AS RPT_SRC_CD_DESC, INVESTIGATION.TRANSMISSION_MODE AS TRANSMISSION_MODE, SUBSTRING(LTRIM(RTRIM(INVESTIGATION.LEGACY_CASE_ID)), 1, 15) AS LEGACY_CASE_ID, ----added on 9/8/2021
			DATE_MM_DD_YYYY AS NOT_SUBMIT_DT
			INTO dbo.TMP_Investigation
			FROM dbo.F_PAGE_CASE AS PAGE_CASE WITH(NOLOCK)---(original table)
				 INNER JOIN
				 dbo.TMP_F_PAGE_CASE AS F_PAGE_CASE
				 ON F_PAGE_CASE.INVESTIGATION_KEY = PAGE_CASE.INVESTIGATION_KEY--(myTable)
				 INNER JOIN
				 TMP_CONDITION AS T
				 ON F_PAGE_CASE.CONDITION_KEY = T.CONDITION_KEY---(my table)
				 INNER JOIN
				 dbo.D_PATIENT WITH(NOLOCK)
				 ON F_PAGE_CASE.patient_key = D_PATIENT.patient_key
				 INNER JOIN
				 dbo.INVESTIGATION WITH(NOLOCK)
				 ON F_PAGE_CASE.INVESTIGATION_KEY = INVESTIGATION.INVESTIGATION_KEY
				 LEFT OUTER JOIN
				 dbo.NOTIFICATION_EVENT WITH(NOLOCK)
				 ON NOTIFICATION_EVENT.PATIENT_KEY = F_PAGE_CASE.PATIENT_KEY
				 LEFT OUTER JOIN
				 dbo.RDB_DATE WITH(NOLOCK)
				 ON NOTIFICATION_EVENT.NOTIFICATION_SUBMIT_DT_KEY = DATE_KEY
				 ORDER BY PAGE_CASE.INVESTIGATION_key;

/*
																							CASE_RPT_MMWR_WEEK= INPUT(CASE_RPT_MMWR_WK, comma20.);
																							CASE_RPT_MMWR_YEAR= INPUT(CASE_RPT_MMWR_YR, comma20.);*/

			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;
			---------------------------------------------------23. CREATE TABLE TMP_HEP_PAT_PROV ---------------------------
			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating  dbo.TMP_HEP_PAT_PROV';
			SET @Proc_Step_no = 23;
			IF OBJECT_ID('dbo.TMP_HEP_PAT_PROV', 'U') IS NOT NULL
			BEGIN
				DROP TABLE TMP_HEP_PAT_PROV;
			END;
			SELECT DISTINCT 
				   TMP_F_PAGE_CASE.INVESTIGATION_KEY AS HEP_PAT_PROV_INV_KEY, P.PROVIDER_LOCAL_ID, P.PROVIDER_FIRST_NAME AS PHYSICIAN_FIRST_NM, P.PROVIDER_MIDDLE_NAME AS PHYSICIAN_MIDDLE_NM, P.PROVIDER_LAST_NAME AS PHYSICIAN_LAST_NM, CAST(NULL AS varchar(300)) AS PHYS_NAME, P.PROVIDER_CITY AS PHYS_CITY, P.PROVIDER_STATE AS PHYS_STATE, P.PROVIDER_COUNTY AS PHYS_COUNTY, CAST(NULL AS varchar(300)) AS PHYSICIAN_ADDRESS_USE_DESC, CAST(NULL AS varchar(300)) AS PHYSICIAN_ADDRESS_TYPE_DESC, P.PROVIDER_ADD_TIME, P.PROVIDER_LAST_CHANGE_TIME, P.PROVIDER_UID AS PHYSICIAN_UID, INVGTR.PROVIDER_FIRST_NAME AS INVESTIGATOR_FIRST_NM, INVGTR.PROVIDER_MIDDLE_NAME AS INVESTIGATOR_MIDDLE_NM, INVGTR.PROVIDER_LAST_NAME AS INVESTIGATOR_LAST_NM, CAST(NULL AS varchar(300)) AS INVESTIGATOR_NAME, INVGTR.PROVIDER_UID AS INVESTIGATOR_UID, REPTORG.ORGANIZATION_NAME AS RPT_SRC_SOURCE_NM, REPTORG.ORGANIZATION_COUNTY_CODE AS RPT_SRC_COUNTY_CD, REPTORG.ORGANIZATION_COUNTY AS RPT_SRC_COUNTY, REPTORG.ORGANIZATION_CITY AS RPT_SRC_CITY, REPTORG.ORGANIZATION_STATE AS RPT_SRC_STATE, CAST(NULL AS varchar(300)) AS REPORTING_SOURCE_ADDRESS_USE, CAST(NULL AS varchar(300)) AS REPORTING_SOURCE_ADDRESS_TYPE, REPTORG.ORGANIZATION_UID AS REPORTING_SOURCE_UID
			INTO dbo.TMP_HEP_PAT_PROV
			FROM dbo.F_PAGE_CASE AS PAGE_CASE WITH(NOLOCK)
				 INNER JOIN
				 TMP_F_PAGE_CASE
				 ON TMP_F_PAGE_CASE.INVESTIGATION_KEY = PAGE_CASE.INVESTIGATION_KEY------ (My table)
				 INNER JOIN
				 TMP_CONDITION AS T
				 ON TMP_F_PAGE_CASE.CONDITION_KEY = T.CONDITION_KEY------ (My table)
				 LEFT OUTER JOIN
				 dbo.D_PROVIDER AS P WITH(NOLOCK)
				 ON PAGE_CASE.PHYSICIAN_KEY = P.PROVIDER_KEY
				 LEFT OUTER JOIN
				 dbo.D_PROVIDER AS INVGTR WITH(NOLOCK)
				 ON PAGE_CASE.INVESTIGATOR_KEY = INVGTR.PROVIDER_KEY
				 LEFT OUTER JOIN
				 dbo.D_ORGANIZATION AS REPTORG WITH(NOLOCK)
				 ON PAGE_CASE.ORG_AS_REPORTER_KEY = REPTORG.ORGANIZATION_KEY
				 ORDER BY HEP_PAT_PROV_INV_KEY;
			UPDATE dbo.TMP_HEP_PAT_PROV
			  SET PHYS_NAME = RTRIM(LTRIM(PHYSICIAN_first_nm));
			UPDATE dbo.TMP_HEP_PAT_PROV
			  SET PHYS_NAME = CASE
							  WHEN PHYS_NAME IS NOT NULL THEN CONCAT(PHYSICIAN_Last_nm, ', ', RTRIM(LTRIM(PHYSICIAN_first_nm)), ' ', PHYSICIAN_middle_nm)
							  ELSE PHYS_NAME
							  END;
			UPDATE dbo.TMP_HEP_PAT_PROV
			  SET PHYS_NAME = CASE
							  WHEN LEN(PHYSICIAN_middle_nm) > 0 THEN PHYS_NAME
							  ELSE RTRIM(LTRIM(PHYS_NAME))
							  END; ----5-17-2021

			UPDATE dbo.TMP_HEP_PAT_PROV
			  SET INVESTIGATOR_NAME = RTRIM(LTRIM(INVESTIGATOR_FIRST_NM));
			UPDATE dbo.TMP_HEP_PAT_PROV
			  SET INVESTIGATOR_NAME = CASE
									  WHEN INVESTIGATOR_NAME IS NOT NULL THEN CONCAT(INVESTIGATOR_Last_nm, ', ', RTRIM(LTRIM(INVESTIGATOR_FIRST_NM)), ' ', INVESTIGATOR_MIDDLE_NM)
									  ELSE INVESTIGATOR_NAME
									  END;
			UPDATE dbo.TMP_HEP_PAT_PROV
			  SET INVESTIGATOR_NAME = CASE
									  WHEN LEN(INVESTIGATOR_middle_nm) > 0 THEN INVESTIGATOR_NAME
									  ELSE RTRIM(LTRIM(INVESTIGATOR_NAME))
									  END; ----5-21-2021

			UPDATE dbo.TMP_HEP_PAT_PROV
			--SET PHYSICIAN_ADDRESS_USE_DESC =concat_ws(' ',RTRIM(LTRIM(PHYS_CITY)),RTRIM(LTRIM(PHYS_STATE)),RTRIM(LTRIM(PHYS_COUNTY)))
			  SET PHYSICIAN_ADDRESS_USE_DESC = concat(RTRIM(LTRIM(ISNULL(PHYS_CITY, ''))), ' ', RTRIM(LTRIM(ISNULL(PHYS_STATE, ''))), ' ', RTRIM(LTRIM(ISNULL(PHYS_COUNTY, ''))));---8-31-2021

			UPDATE dbo.TMP_HEP_PAT_PROV
			  SET PHYSICIAN_ADDRESS_USE_DESC = CASE
											   WHEN LEN(PHYSICIAN_ADDRESS_USE_DESC) > 0 THEN 'Primary Work Place'
											   ELSE NULL
											   END;
			UPDATE dbo.TMP_HEP_PAT_PROV
			--SET PHYSICIAN_ADDRESS_TYPE_DESC =concat_ws(' ',RTRIM(LTRIM(RPT_SRC_COUNTY)),RTRIM(LTRIM(RPT_SRC_STATE)),RTRIM(LTRIM(RPT_SRC_CITY)))
			  SET PHYSICIAN_ADDRESS_TYPE_DESC = concat(RTRIM(LTRIM(ISNULL(RPT_SRC_COUNTY, ''))), ' ', RTRIM(LTRIM(ISNULL(RPT_SRC_STATE, ''))), ' ', RTRIM(LTRIM(ISNULL(RPT_SRC_CITY, ''))));---8-31-2021

			UPDATE dbo.TMP_HEP_PAT_PROV
			  SET PHYSICIAN_ADDRESS_TYPE_DESC = CASE
												WHEN LEN(PHYSICIAN_ADDRESS_TYPE_DESC) > 0 THEN 'Office'
												ELSE NULL
												END;
			UPDATE dbo.TMP_HEP_PAT_PROV
			---SET REPORTING_SOURCE_ADDRESS_USE =concat_ws(' ',RTRIM(LTRIM(PHYS_CITY)),RTRIM(LTRIM(PHYS_STATE)),RTRIM(LTRIM(PHYS_COUNTY)))
			  SET REPORTING_SOURCE_ADDRESS_USE = concat(RTRIM(LTRIM(ISNULL(PHYS_CITY, ''))), ' ', RTRIM(LTRIM(ISNULL(PHYS_STATE, ''))), ' ', RTRIM(LTRIM(ISNULL(PHYS_COUNTY, ''))));---8-31-2021

			UPDATE dbo.TMP_HEP_PAT_PROV
			  SET REPORTING_SOURCE_ADDRESS_USE = CASE
												 WHEN LEN(REPORTING_SOURCE_ADDRESS_USE) > 0 THEN 'Primary Work Place'
												 ELSE NULL
												 END;
			UPDATE dbo.TMP_HEP_PAT_PROV
			---SET REPORTING_SOURCE_ADDRESS_TYPE =concat_ws(' ',RTRIM(LTRIM(RPT_SRC_COUNTY)),RTRIM(LTRIM(RPT_SRC_STATE)),RTRIM(LTRIM(RPT_SRC_CITY)))
			  SET REPORTING_SOURCE_ADDRESS_TYPE = concat(RTRIM(LTRIM(ISNULL(RPT_SRC_COUNTY, ''))), ' ', RTRIM(LTRIM(ISNULL(RPT_SRC_STATE, ''))), ' ', RTRIM(LTRIM(ISNULL(RPT_SRC_CITY, ''))));---8-31-2021

			UPDATE dbo.TMP_HEP_PAT_PROV
			  SET REPORTING_SOURCE_ADDRESS_TYPE = CASE
												  WHEN LEN(REPORTING_SOURCE_ADDRESS_TYPE) > 0 THEN 'office'
												  ELSE NULL
												  END;
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;
			-------------------------------------------------24. CREATE TABLE TMP_D_INVESTIGATION_REPEAT ---------------------------
			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating  dbo.TMP_D_INVESTIGATION_REPEAT';------is same as dataset- D_INVESTIGATION_REPEAT
			SET @Proc_Step_no = 24;
			IF OBJECT_ID('dbo.TMP_F_INVESTIGATION_REPEAT', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_F_INVESTIGATION_REPEAT;
			END;
			IF OBJECT_ID('dbo.TMP_D_INVESTIGATION_REPEAT', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_INVESTIGATION_REPEAT;
			END;
			SELECT PAGE_CASE.D_INVESTIGATION_REPEAT_KEY, PAGE_CASE.INVESTIGATION_KEY, TMP_F_PAGE_CASE.CONDITION_KEY
			INTO dbo.TMP_F_INVESTIGATION_REPEAT
			FROM dbo.F_PAGE_CASE AS PAGE_CASE WITH(NOLOCK)
				 INNER JOIN
				 TMP_F_PAGE_CASE
				 ON TMP_F_PAGE_CASE.INVESTIGATION_KEY = PAGE_CASE.INVESTIGATION_KEY   ----(my table) since in sas it is dbo
				 ORDER BY D_INVESTIGATION_REPEAT_KEY;
			SELECT DISTINCT 
				   F.D_INVESTIGATION_REPEAT_KEY, F.INVESTIGATION_KEY, F.CONDITION_KEY, VAC_VaccineDoseNum, VAC_VaccinationDate, PAGE_CASE_UID, BLOCK_NM, ANSWER_GROUP_SEQ_NBR
			INTO dbo.TMP_D_INVESTIGATION_REPEAT
			FROM dbo.TMP_F_INVESTIGATION_REPEAT AS F
				 LEFT JOIN
				 [dbo].[D_INVESTIGATION_REPEAT] AS D WITH(NOLOCK)
				 ON D.D_INVESTIGATION_REPEAT_KEY = F.D_INVESTIGATION_REPEAT_KEY
				 ORDER BY F.INVESTIGATION_KEY;
			UPDATE dbo.TMP_D_INVESTIGATION_REPEAT
			  SET VAC_VaccinationDate = CASE
										WHEN VAC_VaccinationDate IS NULL THEN NULL
										ELSE VAC_VaccinationDate
										END, [VAC_VACCINEDOSENUM] = CASE
																	WHEN VAC_VaccineDoseNum IS NULL THEN NULL
																	ELSE RTRIM(LTRIM(VAC_VaccineDoseNum))
																	END;
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;

			--------------------------------------------------25. CREATE TABLE TMP_METADATA_TEST---------------------------

			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating dbo.TMP_METADATA_TEST';
			SET @Proc_Step_no = 25;
			IF OBJECT_ID('dbo.TMP_METADATA_TEST', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_METADATA_TEST;
			END;
			SELECT C.CONDITION_KEY, M.block_nm, M.investigation_form_cd
			INTO dbo.TMP_METADATA_TEST
			FROM nbs_odse.dbo.NBS_UI_METADATA AS M WITH(NOLOCK)
				 INNER JOIN
				 TMP_CONDITION AS C
				 ON M.INVESTIGATION_FORM_CD = C.DISEASE_GRP_DESC ----(My table)
			WHERE M.question_identifier IN( 'VAC103', 'VAC120' ) AND 
				  M.[block_nm] IS NOT NULL;
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;

			--------------------------------------------------26a. CREATE TABLE TMP_VAC_REPEAT------------------------------------------------------

			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating dbo.TMP_VAC_REPEAT';
			SET @Proc_Step_no = 26;-----a

			IF OBJECT_ID('dbo.TMP_VAC_REPEAT', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_VAC_REPEAT;
			END;
			SELECT DISTINCT 
				   D.D_INVESTIGATION_REPEAT_KEY, D.INVESTIGATION_KEY, VAC_VaccinationDate, VAC_VaccineDoseNum, D.PAGE_CASE_UID, D.ANSWER_GROUP_SEQ_NBR AS SEQNBR, D.BLOCK_NM, CAST(NULL AS [varchar](300)) AS VAC_GT_4_IND, -----Date Indicator
				   CAST(NULL AS [varchar](300)) AS VAC_DOSE_4_IND, ---Dose Indicator
				   CAST(NULL AS [varchar](300)) AS VACC_GT_4_IND   ---------FinalIndicator
			INTO dbo.TMP_VAC_REPEAT
			FROM dbo.TMP_D_INVESTIGATION_REPEAT AS D
				 INNER JOIN
				 [dbo].TMP_CONDITION AS C
				 ON C.CONDITION_KEY = D.CONDITION_KEY----My Table
				 INNER JOIN
				 dbo.TMP_METADATA_TEST AS M
				 ON M.CONDITION_KEY = D.CONDITION_KEY
			WHERE M.block_nm = D.BLOCK_NM AND 
				  M.block_nm IN
			(
				SELECT DISTINCT 
					   BLOCK_NM
				FROM nbs_odse.dbo.NBS_UI_METADATA WITH(NOLOCK)
				WHERE QUESTION_IDENTIFIER IN( 'VAC103', 'VAC120' ) AND 
					  BLOCK_NM IS NOT NULL
			)
			ORDER BY PAGE_CASE_UID;
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;

			---------------------------------------------------26b. CREATE TABLE TMP_VAC_REPEAT_OUT_DATE_Pivot---------------------------
			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating TMP_VAC_REPEAT_OUT_DATE_Pivot';
			SET @Proc_Step_no = 26;----b

			IF OBJECT_ID('dbo.TMP_VAC_REPEAT_OUT_DATE_Pivot', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_VAC_REPEAT_OUT_DATE_Pivot;
			END;
			IF OBJECT_ID('dbo.TMP_VAC_REPEAT_OUT_DATE', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_VAC_REPEAT_OUT_DATE;
			END;
			IF OBJECT_ID('dbo.TMP_VAC_REPEAT_OUT_DATE_Final', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_VAC_REPEAT_OUT_DATE_Final;
			END;
			DECLARE @cols AS nvarchar(max)= STUFF(
			(
				SELECT DISTINCT 
					   ',[' + CAST(NUM AS varchar) + ']'
				FROM
				(
					SELECT ROW_NUMBER() OVER(
					ORDER BY(SELECT NULL)) AS num
					FROM Sys.Objects
				) AS X
				WHERE num <= 5 FOR XML PATH('')
			), 1, 1, '');
			PRINT @cols;
			DECLARE @SqlCmd nvarchar(max)= '';
			SET @SqlCmd = '
																						SELECT 
																						 D_INVESTIGATION_REPEAT_KEY,INVESTIGATION_KEY,Page_Case_UId, 
																							[1] as VACC_RECVD_DT_1,
																							[2] as VACC_RECVD_DT_2,
																							[3] as VACC_RECVD_DT_3,
																							[4] as VACC_RECVD_DT_4,
																							[5] as VACC_RECVD_DT_5
												
																						 Into dbo.TMP_VAC_REPEAT_OUT_DATE_Pivot  FROM 
																						(
																						SELECT p.*
																						 FROM
																						 (
																							SELECT D_INVESTIGATION_REPEAT_KEY,VAC_VaccinationDate,SEQNBR,INVESTIGATION_KEY, Page_Case_UId
																							FROM dbo.TMP_VAC_REPEAT
																						 ) AS tbl
																						 PIVOT
																						 (
																							MAX(VAC_VaccinationDate) FOR SEQNBR IN(' + @cols + ')
																						 ) AS p)
																						 as c
																						 ';
			PRINT @SqlCmd;
			EXEC sp_executesql @SqlCmd;
			SELECT *, CAST(NULL AS [varchar](300)) AS VAC_GT_4_IND      -----Date Indicator---change the length to 300 on 5-13-2021

			INTO TMP_VAC_REPEAT_OUT_DATE
			FROM TMP_VAC_REPEAT_OUT_DATE_Pivot
			WHERE LEN(VACC_RECVD_DT_1) > 0 OR 
				  LEN(VACC_RECVD_DT_2) > 0 OR 
				  LEN(VACC_RECVD_DT_3) > 0 OR 
				  LEN(VACC_RECVD_DT_4) > 0 OR 
				  LEN(VACC_RECVD_DT_5) > 0 AND 
				  PAGE_CASE_UID > 0;
			UPDATE TMP_VAC_REPEAT_OUT_DATE
			  SET VAC_GT_4_IND = CASE
								 WHEN VACC_RECVD_DT_5 IS NULL THEN NULL
								 ELSE 'True'
								 END;
			SELECT DISTINCT 
				   Page_Case_UId, D_INVESTIGATION_REPEAT_KEY, INVESTIGATION_KEY, VACC_RECVD_DT_1, VACC_RECVD_DT_2, VACC_RECVD_DT_3, VACC_RECVD_DT_4, VAC_GT_4_IND
			INTO TMP_VAC_REPEAT_OUT_DATE_Final
			FROM TMP_VAC_REPEAT_OUT_DATE
			ORDER BY D_INVESTIGATION_REPEAT_KEY;
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;
			---------------------------------------26c. CREATE TABLE TMP_VAC_REPEAT_OUT_NUM_Pivot-------------------------------------------------------------
			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating TMP_VAC_REPEAT_OUT_NUM_Pivot';
			SET @Proc_Step_no = 26;----c

			IF OBJECT_ID('dbo.TMP_VAC_REPEAT_OUT_NUM_Pivot', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_VAC_REPEAT_OUT_NUM_Pivot;
			END;
			IF OBJECT_ID('dbo.TMP_VAC_REPEAT_OUT_NUM', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_VAC_REPEAT_OUT_NUM;
			END;
			IF OBJECT_ID('dbo.TMP_VAC_REPEAT_OUT_NUM_final', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_VAC_REPEAT_OUT_NUM_Final;
			END;
			DECLARE @col AS nvarchar(max)= STUFF(
			(
				SELECT DISTINCT 
					   ',[' + CAST(NUM AS varchar) + ']'
				FROM
				(
					SELECT ROW_NUMBER() OVER(
					ORDER BY(SELECT NULL)) AS num
					FROM Sys.Objects
				) AS X
				WHERE num <= 5 FOR XML PATH('')
			), 1, 1, '');
			PRINT @col;
			DECLARE @SqlCmds nvarchar(max)= '';
			SET @SqlCmds = '
																						SELECT 
																						 D_INVESTIGATION_REPEAT_KEY,INVESTIGATION_KEY,Page_Case_UId, 
																							[1] as VACC_DOSE_NBR_1,
																							[2] as VACC_DOSE_NBR_2,
																							[3] as VACC_DOSE_NBR_3,
																							[4] as VACC_DOSE_NBR_4,
																							[5] as VACC_DOSE_NBR_5
												
																						 Into dbo.TMP_VAC_REPEAT_OUT_NUM_Pivot  FROM 
																						(
																						SELECT p.*
																						 FROM
																						 (
																							SELECT  D_INVESTIGATION_REPEAT_KEY,VAC_VaccineDoseNum,SEQNBR,INVESTIGATION_KEY, Page_Case_UId
																							FROM dbo.TMP_VAC_REPEAT
																						 ) AS tbl
																						 PIVOT
																						 (
																							MAX(VAC_VaccineDoseNum) FOR SEQNBR IN(' + @col + ')
																						 ) AS p)
																						 as c
																						 ';
			PRINT @SqlCmds;
			EXEC sp_executesql @SqlCmds;
			SELECT *, CAST(NULL AS [varchar](2000)) AS VAC_DOSE_4_IND      ---Dose Indicator
			INTO TMP_VAC_REPEAT_OUT_NUM
			FROM TMP_VAC_REPEAT_OUT_NUM_Pivot
			WHERE LEN(VACC_DOSE_NBR_1) > 0 OR 
				  LEN(VACC_DOSE_NBR_2) > 0 OR 
				  LEN(VACC_DOSE_NBR_3) > 0 OR 
				  LEN(VACC_DOSE_NBR_4) > 0 OR 
				  LEN(VACC_DOSE_NBR_5) > 0 AND 
				  PAGE_CASE_UID > 0;
			UPDATE TMP_VAC_REPEAT_OUT_NUM
			  SET VAC_DOSE_4_IND = CASE
								   WHEN VACC_DOSE_NBR_5 IS NULL THEN NULL
								   ELSE 'True'
								   END;
			SELECT DISTINCT 
				   Page_Case_UId, D_INVESTIGATION_REPEAT_KEY, INVESTIGATION_KEY, VACC_DOSE_NBR_1, VACC_DOSE_NBR_2, VACC_DOSE_NBR_3, VACC_DOSE_NBR_4, VAC_DOSE_4_IND
			INTO TMP_VAC_REPEAT_OUT_NUM_Final
			FROM TMP_VAC_REPEAT_OUT_NUM
			ORDER BY D_INVESTIGATION_REPEAT_KEY;
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;

			-------------------------------------------------------26d. CREATE TABLE TMP_VAC_REPEAT_OUT_FINAL1--------------------------------------------------------------------------
			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating TMP_VAC_REPEAT_OUT_FINAL1';
			SET @Proc_Step_no = 26;----d

			IF OBJECT_ID('dbo.TMP_VAC_REPEAT_OUT_FINAL1', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_VAC_REPEAT_OUT_FINAL1;
			END;

/*
																				DATA VAC_REPEAT_OUT_FINAL;
																				MERGE  VAC_REPEAT_OUT_DATE(IN=in1) VAC_REPEAT_OUT_NUM(IN=in2);
																				BY D_INVESTIGATION_REPEAT_KEY;
																				*/

			SELECT DISTINCT 
				   D.Page_Case_UId, D.D_INVESTIGATION_REPEAT_KEY, D.INVESTIGATION_KEY, VACC_DOSE_NBR_1, VACC_RECVD_DT_1, VACC_DOSE_NBR_2, VACC_RECVD_DT_2, VACC_DOSE_NBR_3, VACC_RECVD_DT_3, VACC_DOSE_NBR_4, VACC_RECVD_DT_4, VAC_GT_4_IND, VAC_DOSE_4_IND, CAST(NULL AS [varchar](300)) AS VACC_GT_4_IND   ---------FinalIndicator
			INTO TMP_VAC_REPEAT_OUT_FINAL1
			FROM TMP_VAC_REPEAT_OUT_Date_Final AS D
				 INNER JOIN
				 TMP_VAC_REPEAT_OUT_NUM_Final AS N
				 ON D.D_INVESTIGATION_REPEAT_KEY = N.D_INVESTIGATION_REPEAT_KEY
				 ORDER BY D.D_INVESTIGATION_REPEAT_KEY;
			UPDATE TMP_VAC_REPEAT_OUT_FINAL1
			  SET VACC_GT_4_IND = CASE
								  WHEN LEN(RTRIM(VAC_DOSE_4_IND)) > 0 OR 
									   LEN(RTRIM(VAC_GT_4_IND)) > 0 THEN 'True'
								  ELSE NULL
								  END;
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;
			--------------------------------------------------26e. CREATE TABLE TMP_VAC_REPEAT_OUT_FINAL--------------------------------------------------------------------------
			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating TMP_VAC_REPEAT_OUT_FINAL';
			SET @Proc_Step_no = 26;----e

			IF OBJECT_ID('dbo.TMP_VAC_REPEAT_OUT_FINAL', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_VAC_REPEAT_OUT_FINAL;
			END;
			SELECT Page_Case_UId, D_INVESTIGATION_REPEAT_KEY, INVESTIGATION_KEY, VACC_DOSE_NBR_1, VACC_RECVD_DT_1, VACC_DOSE_NBR_2, VACC_RECVD_DT_2, VACC_DOSE_NBR_3, VACC_RECVD_DT_3, VACC_DOSE_NBR_4, VACC_RECVD_DT_4, VACC_GT_4_IND   ---------FinalIndicator
			INTO dbo.TMP_VAC_REPEAT_OUT_FINAL
			FROM dbo.TMP_VAC_REPEAT_OUT_FINAL1;
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;	
			---------------------------------------------------------Final Table 27 TMP_HEPATITIS_CASE_BASE------------------------------------------------------------------------------------------------

			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Generating TMP_HEPATITIS_CASE_BASE';
			SET @Proc_Step_no = 27;
			IF OBJECT_ID('dbo.TMP_HEPATITIS_CASE_BASE', 'U') IS NOT NULL
			BEGIN
				DROP TABLE TMP_HEPATITIS_CASE_BASE;
			END;
			SELECT DISTINCT 
				   A.INIT_NND_NOT_DT, ---1
				   I.CASE_RPT_MMWR_WEEK, ---2
				   I.CASE_RPT_MMWR_YEAR, ----3
				   C.HEP_D_INFECTION_IND, ----4
				   C.HEP_MEDS_RECVD_IND, ----5
				   L.HEP_C_TOTAL_ANTIBODY, ----6
				   I.DIAGNOSIS_DT, ------------7
				   I.DIE_FRM_THIS_ILLNESS_IND, ----8
				   I.DISEASE_IMPORTED_IND, -----9
				   I.EARLIEST_RPT_TO_CNTY, ----10
				   I.EARLIEST_RPT_TO_STATE_DT, ---11
				   RTRIM(LTRIM(SUBSTRING(A.BINATIONAL_RPTNG_CRIT,1,300))) as BINATIONAL_RPTNG_CRIT, ---12
				   E.CHILDCARE_CASE_IND, ---13
				   E.CNTRY_USUAL_RESIDENCE, ---14
				   E.CT_BABYSITTER_IND, ---15
				   E.CT_CHILDCARE_IND, ----16
				   E.CT_HOUSEHOLD_IND, ---17
				   E.HEP_CONTACT_IND, ----18
				   R.HEP_CONTACT_EVER_IND, ----19
				   E.OTHER_CONTACT_IND, ---20
				   ISNULL(NULLIF(E.COM_SRC_OUTBREAK_IND, ''), NULL) AS COM_SRC_OUTBREAK_IND, ---21
				   E.CONTACT_TYPE_OTH, ----22
				   E.CT_PLAYMATE_IND, ----23
				   E.SEXUAL_PARTNER_IND, ----24
				   E.DNP_HOUSEHOLD_CT_IND, ---25
				   E.HEP_A_EPLINK_IND, ----25
				   E.FEMALE_SEX_PRTNR_NBR, ---27
				   E.FOODHNDLR_PRIOR_IND, ----28
				   ISNULL(NULLIF(E.DNP_EMPLOYEE_IND, ''), NULL) AS DNP_EMPLOYEE_IND, -----29
				   ISNULL(NULLIF(E.STREET_DRUG_INJECTED, ''), NULL) AS STREET_DRUG_INJECTED, -----30
				   ISNULL(NULLIF(E.MALE_SEX_PRTNR_NBR, ''), NULL) AS MALE_SEX_PRTNR_NBR, ----31
				   I.OUTBREAK_IND, -----32
				   ISNULL(NULLIF(E.OBRK_FOODHNDLR_IND, ''), NULL) AS OBRK_FOODHNDLR_IND, ----33
				   ISNULL(NULLIF(E.FOOD_OBRK_FOOD_ITEM, ''), NULL) AS FOOD_OBRK_FOOD_ITEM, ----34
				   ISNULL(NULLIF(E.OBRK_NOFOODHNDLR_IND, ''), NULL) AS OBRK_NOFOODHNDLR_IND, ----35
				   ISNULL(NULLIF(E.OBRK_UNIDENTIFIED_IND, ''), NULL) AS OBRK_UNIDENTIFIED_IND, ----36
				   ISNULL(NULLIF(E.OBRK_WATERBORNE_IND, ''), NULL) AS OBRK_WATERBORNE_IND, ----37
				   ISNULL(NULLIF(E.STREET_DRUG_USED, ''), NULL) AS STREET_DRUG_USED, ---38
				   PO.SEX_PREF, ----39
				   I.HSPTL_ADMISSION_DT, ----40
				   I.HSPTL_DISCHARGE_DT, ----41
				   I.HSPTL_DURATION_DAYS, ----42
				   I.HSPTLIZD_IND, ---43
				   I.ILLNESS_ONSET_DT, ----44
				   I.INV_CASE_STATUS, ----45
				   ----	ltrim(rtrim(I.INV_COMMENTS)) as INV_COMMENTS,----46--------7/21/2021
				   LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE([INV_COMMENTS], CHAR(10), CHAR(32)), CHAR(13), CHAR(32)), CHAR(160), CHAR(32)), CHAR(9), CHAR(32)))) AS INV_COMMENTS, ----46--------7/21/2021
				   I.INV_LOCAL_ID, ---47
				   I.INV_RPT_DT, ---48
				   I.INV_START_DT, ---49
				   I.INVESTIGATION_STATUS, ---50
				   I.JURISDICTION_NM, ---51
				   L.ALT_SGPT_RESULT, ----52
				   L.ANTI_HBS_POS_REAC_IND, -----53
				   L.AST_SGOT_RESULT, ---54
				   L.HEP_E_ANTIGEN, ----55
				   L.HBE_AG_DT, ---56
				   L.HEP_B_SURFACE_ANTIGEN, ----57
				   L.HBS_AG_DT, ----58
				   L.HEP_B_DNA, -----59
				   L.HBV_NAT_DT, ----60	
				   L.HCV_RNA, ----61
				   L.HCV_RNA_DT, ----62
				   L.HEP_D_TEST_IND, ----63
				   L.HEP_A_IGM_ANTIBODY, ----64
				   L.IGM_ANTI_HAV_DT, ----65
				   L.HEP_B_IGM_ANTIBODY, ----66
				   L.IGM_ANTI_HBC_DT, ----67
				   L.PREV_NEG_HEP_TEST_IND, ------68
				   L.ANTIHCV_SIGCUT_RATIO, ----69
				   L.ANTIHCV_SUPP_ASSAY, -----70
				   L.SUPP_ANTI_HCV_DT, -----71
				   L.ALT_RESULT_DT, ----72
				   L.AST_RESULT_DT, -----73
				   L.ALT_SGPT_RSLT_UP_LMT, ----74
				   L.AST_SGOT_RSLT_UP_LMT, ----75
				   L.HEP_A_TOTAL_ANTIBODY, ----76
				   L.TOTAL_ANTI_HAV_DT, ----77
				   L.HEP_B_TOTAL_ANTIBODY, ----78
				   L.TOTAL_ANTI_HBC_DT, ----79
				   L.TOTAL_ANTI_HCV_DT, ----80
				   L.HEP_D_TOTAL_ANTIBODY, ----81
				   L.TOTAL_ANTI_HDV_DT, ----82
				   L.HEP_E_TOTAL_ANTIBODY, ----83
				   L.TOTAL_ANTI_HEV_DT, ----84
				   L.VERIFIED_TEST_DT, ----85
				   I.LEGACY_CASE_ID, ---86
				   MH.DIABETES_IND, ----87
				   MH.DIABETES_DX_DT, ----88
				   MH.PREGNANCY_DUE_DT, ----89
				   MH.PAT_JUNDICED_IND, -----90
				   MH.PAT_PREV_AWARE_IND, ----91
				   MH.HEP_CARE_PROVIDER, ----92
				   MH.TEST_REASON, ------93
				   RTRIM(LTRIM(SUBSTRING(MH.TEST_REASON_OTH,1,150))) as TEST_REASON_OTH, -----94----10/18/2021 added the trim since the dest has only 150 length
				   MH.SYMPTOMATIC_IND, -----95
				   M.MTH_BORN_OUTSIDE_US, ---96
				   M.MTH_ETHNICITY, ------97
				   M.MTH_HBS_AG_PRIOR_POS, ----98
				   M.MTH_POS_AFTER, ------99
				   M.MTH_POS_TEST_DT, ----100
				   M.MTH_RACE, -----101
				   M.MTH_BIRTH_COUNTRY, ----102
				   I.NOT_SUBMIT_DT, ----103
				   P.PAT_REPORTED_AGE, ----104
				   P.PAT_REPORTED_AGE_UNIT, ----105	
				   P.PAT_CITY, ----106
				   P.PAT_COUNTRY, ----107
				   P.PAT_COUNTY, ----108
				   P.PAT_CURR_GENDER, ---109
				   P.PAT_DOB, ---110
				   LTRIM(RTRIM(P.PAT_ETHNICITY)) AS PAT_ETHNICITY, ---111
				   P.PAT_FIRST_NM, ---112
				   P.PAT_LAST_NM, ---113
				   P.PAT_LOCAL_ID, ---114
				   P.PAT_MIDDLE_NM, ---115
				   I.PAT_PREGNANT_IND, ---116
				   P.PAT_RACE, ----117
				   P.PAT_STATE, ----118
				   LTRIM(RTRIM(P.PAT_STREET_ADDR_1)) AS PAT_STREET_ADDR_1, ---119-------------------7/14/2021
				   P.PAT_STREET_ADDR_2, ----120	
				   P.PAT_ZIP_CODE, ----121
				   HP.RPT_SRC_SOURCE_NM, ----122
				   HP.RPT_SRC_STATE, ----123
				   I.RPT_SRC_CD_DESC, ---124
				   R.BLD_EXPOSURE_IND, ----125
				   R.BLD_RECVD_IND, ----126	
				   R.BLD_RECVD_DT, ----127
				   R.MED_DEN_BLD_CT_FRQ, ----128
				   R.MED_DEN_EMP_EVER_IND, ----129
				   R.MED_DEN_EMPLOYEE_IND, ---130
				   R.CLOTFACTOR_PRIOR_1987, ----131	
				   R.BLD_CONTAM_IND, ------132
				   R.DEN_WORK_OR_SURG_IND, ----133
				   R.HEMODIALYSIS_IND, ----134
				   R.LT_HEMODIALYSIS_IND, -----135
				   R.HSPTL_PRIOR_ONSET_IND, ----136
				   R.EVER_INJCT_NOPRSC_DRG, -----137	
				   R.INCAR_24PLUSHRS_IND, ---138
				   R.INCAR_6PLUS_MO_IND, ---139
				   R.EVER_INCAR_IND, ----140
				   R.INCAR_TYPE_JAIL_IND, ----141
				   R.INCAR_TYPE_PRISON_IND, ----142
				   R.INCAR_TYPE_JUV_IND, ----143
				   R.LAST6PLUSMO_INCAR_PER, ----144	
				   R.LAST6PLUSMO_INCAR_YR, ----145
				   R.OUTPAT_IV_INF_IND, ----146
				   R.LTCARE_RESIDENT_IND, ----147
				   R.LIFE_SEX_PRTNR_NBR, ----148
				   R.BLD_EXPOSURE_OTH, ----149
				   R.PIERC_PRIOR_ONSET_IND, ----150	
				   R.PIERC_PERF_LOC_OTH, ----151	
				   R.PIERC_PERF_LOC, -----152
				   R.PUB_SAFETY_BLD_CT_FRQ, ----153	
				   R.PUB_SAFETY_WORKER_IND, ----154	
				   R.STD_TREATED_IND, ----155
				   R.STD_LAST_TREATMENT_YR, ---156	
				   R.NON_ORAL_SURGERY_IND, ---157
				   R.TATT_PRIOR_ONSET_IND, ---158
				   R.TATTOO_PERF_LOC, ----158
				   R.TATT_PRIOR_LOC_OTH, ----160
				   R.BLD_TRANSF_PRIOR_1992, ---161
				   R.ORGN_TRNSP_PRIOR_1992, ----162	
				   I.TRANSMISSION_MODE, ----163
				   T.HOUSEHOLD_TRAVEL_IND, ----164
				   T.TRAVEL_OUT_USACAN_IND, ----165
				   T.TRAVEL_OUT_USACAN_LOC, -----166
				   T.HOUSEHOLD_TRAVEL_LOC, ----167
				   T.TRAVEL_REASON, -----168
				   V.IMM_GLOB_RECVD_IND, ----169
				   V.GLOB_LAST_RECVD_YR, ----170
				   V.VACC_RECVD_IND, ----171-------Has to be 10 digits long
				   VAC.VACC_DOSE_NBR_1, ----172
				   VAC.VACC_RECVD_DT_1, ----173
				   VAC.VACC_DOSE_NBR_2, ----174
				   VAC.VACC_RECVD_DT_2, ----175
				   VAC.VACC_DOSE_NBR_3, ----176
				   VAC.VACC_RECVD_DT_3, -----177
				   VAC.VACC_DOSE_NBR_4, -----178
				   VAC.VACC_RECVD_DT_4, ---179
				   VAC.VACC_GT_4_IND, -----180
				   V.VACC_DOSE_RECVD_NBR, ----181
				   V.VACC_LAST_RECVD_YR, -----182
				   L.ANTI_HBSAG_TESTED_IND, ----183
				   I.CONDITION_CD, -----184
				   CAST(NULL AS datetime) AS EVENT_DATE, ----185
				   I.IMPORT_FROM_CITY, ----186
				   I.IMPORT_FROM_COUNTRY, ----187
				   I.IMPORT_FROM_COUNTY, ----188
				   I.IMPORT_FROM_STATE, ----189
				   I.INVESTIGATION_KEY, -----190
				   HP.INVESTIGATOR_NAME, -----191
				   P.PAT_ELECTRONIC_IND, ----192
				   HP.PHYS_CITY, ----193
				   HP.PHYS_COUNTY, ----194
				   HP.PHYS_NAME, ----195
				   HP.PHYS_STATE, ----196
				   I.PROGRAM_JURISDICTION_OID, ----197
				   HP.RPT_SRC_CITY, ---198
				   HP.RPT_SRC_COUNTY, ----199
				   HP.RPT_SRC_COUNTY_CD, ---200
				   HP.PHYSICIAN_UID, ----201
				   P.PATIENT_UID, ----202
				   I.CASE_UID, ---203
				   HP.INVESTIGATOR_UID, ---204
				   HP.REPORTING_SOURCE_UID, ---205
				   CAST(NULL AS datetime) AS REFRESH_DATETIME, ---206
				   P.PAT_BIRTH_COUNTRY----207

			INTO dbo.TMP_HEPATITIS_CASE_BASE
			FROM dbo.TMP_Investigation AS I WITH(NOLOCK)
				 FULL OUTER JOIN
				 dbo.TMP_D_INV_LAB_FINDING AS L WITH(NOLOCK)
				 ON I.INVESTIGATION_KEY = L.LAB_INV_KEY
				 FULL OUTER JOIN
				 dbo.TMP_D_INV_RISK_FACTOR AS R WITH(NOLOCK)
				 ON I.INVESTIGATION_KEY = R.RISK_INV_KEY
				 FULL OUTER JOIN
				 dbo.TMP_D_INV_EPIDEMIOLOGY AS E WITH(NOLOCK)
				 ON I.INVESTIGATION_KEY = E.EPIDEMIOLOGY_INV_KEY
				 FULL OUTER JOIN
				 dbo.TMP_D_Patient AS P WITH(NOLOCK)
				 ON I.INVESTIGATION_KEY = P.Patient_INV_KEY
				 FULL OUTER JOIN
				 dbo.TMP_D_INV_VACCINATION AS V WITH(NOLOCK)
				 ON I.INVESTIGATION_KEY = V.VACCINATION_INV_KEY
				 FULL OUTER JOIN
				 dbo.TMP_D_INV_TRAVEL AS T WITH(NOLOCK)
				 ON I.INVESTIGATION_KEY = T.Travel_INV_KEY
				 FULL OUTER JOIN
				 dbo.TMP_D_INV_MOTHER AS M WITH(NOLOCK)
				 ON I.INVESTIGATION_KEY = M.Mother_INV_KEY
				 FULL OUTER JOIN
				 dbo.TMP_D_INV_MEDICAL_HISTORY AS MH WITH(NOLOCK)
				 ON I.INVESTIGATION_KEY = MH.MEDHistory_INV_KEY
				 FULL OUTER JOIN
				 dbo.TMP_D_INV_ADMINISTRATIVE AS A WITH(NOLOCK)
				 ON I.INVESTIGATION_KEY = A.ADMIN_INV_KEY
				 FULL OUTER JOIN
				 dbo.TMP_D_INV_PATIENT_OBS AS PO WITH(NOLOCK)
				 ON I.INVESTIGATION_KEY = PO.PATIENT_OBS_INV_KEY
				 FULL OUTER JOIN
				 dbo.TMP_HEP_PAT_PROV AS HP WITH(NOLOCK)
				 ON I.INVESTIGATION_KEY = HP.HEP_PAT_PROV_INV_KEY
				 FULL OUTER JOIN
				 dbo.TMP_D_INV_CLINICAL AS C WITH(NOLOCK)
				 ON I.INVESTIGATION_KEY = C.CLINICAL_INV_KEY
				 FULL OUTER JOIN
				 dbo.TMP_VAC_REPEAT_OUT_FINAL AS VAC WITH(NOLOCK)
				 ON I.INVESTIGATION_KEY = VAC.INVESTIGATION_KEY;-------(--Actually Should be inner Join)

/*data HEPATITIS_CASE_BASE;
																						set HEPATITIS_CASE_BASE;
																						if trim(LENGTHN(VACC_GT_4_IND))<1 then do;
																						VACC_GT_4_IND="FALSE"; end;  
																						run;*/

			---7/14/2021(since getting error converting varchar to int)
			UPDATE dbo.TMP_HEPATITIS_CASE_BASE
			  SET LAST6PLUSMO_INCAR_PER = CASE
										  WHEN LAST6PLUSMO_INCAR_PER NOT LIKE N'%[^0-9.,-]%' AND 
											   LAST6PLUSMO_INCAR_PER NOT LIKE '.' AND 
											   ISNUMERIC(LAST6PLUSMO_INCAR_PER) = 1 THEN LAST6PLUSMO_INCAR_PER
										  ELSE 0
										  END;------------  /*EXT_LAST6PLUSMO_INCAR_PER= INPUT(RSK_IncarcTimeMonths, comma20.)*/----20

			UPDATE dbo.TMP_HEPATITIS_CASE_BASE
			  SET LAST6PLUSMO_INCAR_YR = CASE
										 WHEN LAST6PLUSMO_INCAR_YR NOT LIKE N'%[^0-9.,-]%' AND 
											  LAST6PLUSMO_INCAR_YR NOT LIKE '.' AND 
											  ISNUMERIC(LAST6PLUSMO_INCAR_YR) = 1 THEN LAST6PLUSMO_INCAR_YR
										 ELSE 0
										 END;------------  /*EXT_LAST6PLUSMO_INCAR_PER= INPUT(RSK_IncarcTimeMonths, comma20.)*/----20

			UPDATE dbo.TMP_HEPATITIS_CASE_BASE
			  SET VACC_GT_4_IND = CASE
								  WHEN LEN(ISNULL(RTRIM(LTRIM(VACC_GT_4_IND)), '')) < 1 THEN 'False'
								  ELSE VACC_GT_4_IND
								  END;

/*proc sql;
																					DELETE from HEPATITIS_CASE_BASE where patient_uid is null;
																					quit;*/

			DELETE FROM dbo.TMP_HEPATITIS_CASE_BASE
			WHERE PATIENT_UID IS NULL;
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;
			------------------------------------------------------------------------------------------------------------------------------------------------------

			UPDATE dbo.TMP_HEPATITIS_CASE_BASE
			  SET REFRESH_DATETIME = GETDATE();

			/* Note = FIRST_RPT_TO_CNTY_DT   is EARLIEST_RPT_TO_CNTY */

			----1.---HSPTL_DISCHARGE_DT
			UPDATE dbo.TMP_HEPATITIS_CASE_BASE
			  SET EVENT_DATE = HSPTL_DISCHARGE_DT
			WHERE EVENT_DATE IS NULL OR 
				  EVENT_DATE > HSPTL_DISCHARGE_DT AND 
				  HSPTL_DISCHARGE_DT IS NOT NULL;

			----2.---- HSPTL_ADMISSION_DT
			UPDATE dbo.TMP_HEPATITIS_CASE_BASE
			  SET EVENT_DATE = HSPTL_ADMISSION_DT
			WHERE EVENT_DATE IS NULL OR 
				  EVENT_DATE > HSPTL_ADMISSION_DT AND 
				  HSPTL_ADMISSION_DT IS NOT NULL;

			----3. ---AST_RESULT_DT
			UPDATE dbo.TMP_HEPATITIS_CASE_BASE
			  SET EVENT_DATE = AST_RESULT_DT
			WHERE EVENT_DATE IS NULL OR 
				  EVENT_DATE > AST_RESULT_DT AND 
				  AST_RESULT_DT IS NOT NULL;

			----4.---ALT_RESULT_DT
			UPDATE dbo.TMP_HEPATITIS_CASE_BASE
			  SET EVENT_DATE = ALT_RESULT_DT
			WHERE EVENT_DATE IS NULL OR 
				  EVENT_DATE > ALT_RESULT_DT AND 
				  ALT_RESULT_DT IS NOT NULL;

			----5.--- INV_START_DT
			UPDATE dbo.TMP_HEPATITIS_CASE_BASE
			  SET EVENT_DATE = INV_START_DT
			WHERE EVENT_DATE IS NULL OR 
				  EVENT_DATE > INV_START_DT AND 
				  INV_START_DT IS NOT NULL;

			---6.  ---EARLIEST_RPT_TO_STATE_DT
			UPDATE dbo.TMP_HEPATITIS_CASE_BASE
			  SET EVENT_DATE = EARLIEST_RPT_TO_STATE_DT
			WHERE EVENT_DATE IS NULL OR 
				  EVENT_DATE > EARLIEST_RPT_TO_STATE_DT AND 
				  EARLIEST_RPT_TO_STATE_DT IS NOT NULL;

			----7.----EARLIEST_RPT_TO_CNTY
			UPDATE dbo.TMP_HEPATITIS_CASE_BASE
			  SET EVENT_DATE = EARLIEST_RPT_TO_CNTY
			WHERE EVENT_DATE IS NULL OR 
				  EVENT_DATE > EARLIEST_RPT_TO_CNTY AND 
				  EARLIEST_RPT_TO_CNTY IS NOT NULL;

/*
																								----8---INV_RPT_DT 
																								UPDATE dbo.TMP_HEPATITIS_CASE_BASE 
																								SET EVENT_DATE = INV_RPT_DT
																								Where  EVENT_DATE iS NULL or
																								EVENT_DATE > INV_RPT_DT AND  INV_RPT_DT IS NOT NULL 
																								*/

			----8.----ILLNESS_ONSET_DT---
			UPDATE TMP_HEPATITIS_CASE_BASE
			  SET EVENT_DATE = DIAGNOSIS_DT
			WHERE DIAGNOSIS_DT IS NOT NULL;

			----9.----ILLNESS_ONSET_DT---
			UPDATE TMP_HEPATITIS_CASE_BASE
			  SET EVENT_DATE = ILLNESS_ONSET_DT
			WHERE ILLNESS_ONSET_DT IS NOT NULL;

			----10 If all Dates Are Null get the addTime from Investigation Table ---5-19-2021
			UPDATE TMP_HEPATITIS_CASE_BASE
			  SET EVENT_DATE = ADD_TIME
			FROM [dbo].[INVESTIGATION] I
			WHERE Event_Date IS NULL AND 
				  I.INV_LOCAL_ID = TMP_HEPATITIS_CASE_BASE.INV_LOCAL_ID AND 
				  I.[INVESTIGATION_KEY] = TMP_HEPATITIS_CASE_BASE.INVESTIGATION_KEY;

			----HEP_D_TEST_IND  to appear as N,Y U or Null--added on 5-19-2021
			UPDATE dbo.TMP_HEPATITIS_CASE_BASE ----5/20-2021
			  SET HEP_D_TEST_IND = CASE
								   WHEN HEP_D_TEST_IND IS NULL THEN NULL
								   WHEN HEP_D_TEST_IND = 'Yes' THEN 'Y'
								   WHEN HEP_D_TEST_IND = 'No' THEN 'N'
								   ELSE 'U'
								   END;

			---------------------------------------------------------Final Table 28 TMP_HEPATITIS_CASE_BASE------------------------------------------------------------------------------------------------

			BEGIN TRANSACTION;
			SET @Proc_Step_name = 'Updating HEPATITIS_CASE_BASE';
			SET @Proc_Step_no = 28;
			UPDATE [dbo].[HEPATITIS_DATAMART]
			  SET INIT_NND_NOT_DT = H.[INIT_NND_NOT_DT], CASE_RPT_MMWR_WEEK = H.[CASE_RPT_MMWR_WEEK], CASE_RPT_MMWR_YEAR = H.[CASE_RPT_MMWR_YEAR], HEP_D_INFECTION_IND = H.[HEP_D_INFECTION_IND], HEP_MEDS_RECVD_IND = H.[HEP_MEDS_RECVD_IND], HEP_C_TOTAL_ANTIBODY = H.[HEP_C_TOTAL_ANTIBODY], DIAGNOSIS_DT = H.[DIAGNOSIS_DT], DIE_FRM_THIS_ILLNESS_IND = H.[DIE_FRM_THIS_ILLNESS_IND], DISEASE_IMPORTED_IND = H.[DISEASE_IMPORTED_IND], EARLIEST_RPT_TO_CNTY = H.[EARLIEST_RPT_TO_CNTY], EARLIEST_RPT_TO_STATE_DT = H.[EARLIEST_RPT_TO_STATE_DT], BINATIONAL_RPTNG_CRIT = H.[BINATIONAL_RPTNG_CRIT], CHILDCARE_CASE_IND = H.[CHILDCARE_CASE_IND], CNTRY_USUAL_RESIDENCE = H.[CNTRY_USUAL_RESIDENCE], CT_BABYSITTER_IND = H.[CT_BABYSITTER_IND], CT_CHILDCARE_IND = H.[CT_CHILDCARE_IND], CT_HOUSEHOLD_IND = H.[CT_HOUSEHOLD_IND], HEP_CONTACT_IND = H.[HEP_CONTACT_IND], HEP_CONTACT_EVER_IND = H.[HEP_CONTACT_EVER_IND], OTHER_CONTACT_IND = H.[OTHER_CONTACT_IND], COM_SRC_OUTBREAK_IND = H.[COM_SRC_OUTBREAK_IND], CONTACT_TYPE_OTH = H.[CONTACT_TYPE_OTH], CT_PLAYMATE_IND = H.[CT_PLAYMATE_IND], SEXUAL_PARTNER_IND = H.[SEXUAL_PARTNER_IND], DNP_HOUSEHOLD_CT_IND = H.[DNP_HOUSEHOLD_CT_IND], HEP_A_EPLINK_IND = H.[HEP_A_EPLINK_IND], FEMALE_SEX_PRTNR_NBR = H.[FEMALE_SEX_PRTNR_NBR], FOODHNDLR_PRIOR_IND = H.[FOODHNDLR_PRIOR_IND], DNP_EMPLOYEE_IND = H.[DNP_EMPLOYEE_IND], STREET_DRUG_INJECTED = H.[STREET_DRUG_INJECTED], MALE_SEX_PRTNR_NBR = H.[MALE_SEX_PRTNR_NBR], OUTBREAK_IND = H.[OUTBREAK_IND], OBRK_FOODHNDLR_IND = H.[OBRK_FOODHNDLR_IND], FOOD_OBRK_FOOD_ITEM = H.[FOOD_OBRK_FOOD_ITEM], OBRK_NOFOODHNDLR_IND = H.[OBRK_NOFOODHNDLR_IND], OBRK_UNIDENTIFIED_IND = H.[OBRK_UNIDENTIFIED_IND], OBRK_WATERBORNE_IND = H.[OBRK_WATERBORNE_IND], STREET_DRUG_USED = H.[STREET_DRUG_USED], SEX_PREF = H.[SEX_PREF], HSPTL_ADMISSION_DT = H.[HSPTL_ADMISSION_DT], HSPTL_DISCHARGE_DT = H.[HSPTL_DISCHARGE_DT], HSPTL_DURATION_DAYS = H.[HSPTL_DURATION_DAYS], HSPTLIZD_IND = H.[HSPTLIZD_IND], ILLNESS_ONSET_DT = H.[ILLNESS_ONSET_DT], INV_CASE_STATUS = H.[INV_CASE_STATUS], INV_COMMENTS = H.[INV_COMMENTS], INV_LOCAL_ID = H.[INV_LOCAL_ID], INV_RPT_DT = H.[INV_RPT_DT], INV_START_DT = H.[INV_START_DT], INVESTIGATION_STATUS = H.[INVESTIGATION_STATUS], JURISDICTION_NM = H.[JURISDICTION_NM], ALT_SGPT_RESULT = H.[ALT_SGPT_RESULT], ANTI_HBS_POS_REAC_IND = H.[ANTI_HBS_POS_REAC_IND], AST_SGOT_RESULT = H.[AST_SGOT_RESULT], HEP_E_ANTIGEN = H.[HEP_E_ANTIGEN], HBE_AG_DT = H.[HBE_AG_DT], HEP_B_SURFACE_ANTIGEN = H.[HEP_B_SURFACE_ANTIGEN], HBS_AG_DT = H.[HBS_AG_DT], HBV_NAT_DT = H.[HBV_NAT_DT], HCV_RNA = H.[HCV_RNA], HCV_RNA_DT = H.[HCV_RNA_DT], HEP_D_TEST_IND = H.[HEP_D_TEST_IND], HEP_A_IGM_ANTIBODY = H.[HEP_A_IGM_ANTIBODY], IGM_ANTI_HAV_DT = H.[IGM_ANTI_HAV_DT], HEP_B_IGM_ANTIBODY = H.[HEP_B_IGM_ANTIBODY], IGM_ANTI_HBC_DT = H.[IGM_ANTI_HBC_DT], PREV_NEG_HEP_TEST_IND = H.[PREV_NEG_HEP_TEST_IND], ANTIHCV_SIGCUT_RATIO = H.[ANTIHCV_SIGCUT_RATIO], ANTIHCV_SUPP_ASSAY = H.[ANTIHCV_SUPP_ASSAY], SUPP_ANTI_HCV_DT = H.[SUPP_ANTI_HCV_DT], ALT_RESULT_DT = H.[ALT_RESULT_DT], AST_RESULT_DT = H.[AST_RESULT_DT], ALT_SGPT_RSLT_UP_LMT = H.[ALT_SGPT_RSLT_UP_LMT], AST_SGOT_RSLT_UP_LMT = H.[AST_SGOT_RSLT_UP_LMT], HEP_A_TOTAL_ANTIBODY = H.[HEP_A_TOTAL_ANTIBODY], TOTAL_ANTI_HAV_DT = H.[TOTAL_ANTI_HAV_DT], HEP_B_TOTAL_ANTIBODY = H.[HEP_B_TOTAL_ANTIBODY], TOTAL_ANTI_HBC_DT = H.[TOTAL_ANTI_HBC_DT], TOTAL_ANTI_HCV_DT = H.[TOTAL_ANTI_HCV_DT], HEP_D_TOTAL_ANTIBODY = H.[HEP_D_TOTAL_ANTIBODY], TOTAL_ANTI_HDV_DT = H.[TOTAL_ANTI_HDV_DT], HEP_E_TOTAL_ANTIBODY = H.[HEP_E_TOTAL_ANTIBODY], TOTAL_ANTI_HEV_DT = H.[TOTAL_ANTI_HEV_DT], VERIFIED_TEST_DT = H.[VERIFIED_TEST_DT], LEGACY_CASE_ID = H.[LEGACY_CASE_ID], DIABETES_IND = H.[DIABETES_IND], DIABETES_DX_DT = H.[DIABETES_DX_DT], PREGNANCY_DUE_DT = H.[PREGNANCY_DUE_DT], PAT_JUNDICED_IND = H.[PAT_JUNDICED_IND], PAT_PREV_AWARE_IND = H.[PAT_PREV_AWARE_IND], HEP_CARE_PROVIDER = H.[HEP_CARE_PROVIDER], TEST_REASON = H.[TEST_REASON], TEST_REASON_OTH = H.[TEST_REASON_OTH], SYMPTOMATIC_IND = H.[SYMPTOMATIC_IND], MTH_BORN_OUTSIDE_US = H.[MTH_BORN_OUTSIDE_US], MTH_ETHNICITY = H.[MTH_ETHNICITY], MTH_HBS_AG_PRIOR_POS = H.[MTH_HBS_AG_PRIOR_POS], MTH_POS_AFTER = H.[MTH_POS_AFTER], MTH_POS_TEST_DT = H.[MTH_POS_TEST_DT], MTH_RACE = H.[MTH_RACE], MTH_BIRTH_COUNTRY = H.[MTH_BIRTH_COUNTRY], NOT_SUBMIT_DT = H.[NOT_SUBMIT_DT], PAT_REPORTED_AGE = H.[PAT_REPORTED_AGE], PAT_REPORTED_AGE_UNIT = H.[PAT_REPORTED_AGE_UNIT], PAT_CITY = H.[PAT_CITY], PAT_COUNTRY = H.[PAT_COUNTRY], PAT_COUNTY = H.[PAT_COUNTY], PAT_CURR_GENDER = H.[PAT_CURR_GENDER], PAT_DOB = H.[PAT_DOB], PAT_ETHNICITY = H.[PAT_ETHNICITY], PAT_FIRST_NM = H.[PAT_FIRST_NM], PAT_LAST_NM = H.[PAT_LAST_NM], PAT_LOCAL_ID = H.[PAT_LOCAL_ID], PAT_MIDDLE_NM = H.[PAT_MIDDLE_NM], PAT_PREGNANT_IND = H.[PAT_PREGNANT_IND], PAT_RACE = H.[PAT_RACE], PAT_STATE = H.[PAT_STATE], PAT_STREET_ADDR_1 = H.[PAT_STREET_ADDR_1], PAT_STREET_ADDR_2 = H.[PAT_STREET_ADDR_2], PAT_ZIP_CODE = H.[PAT_ZIP_CODE], RPT_SRC_SOURCE_NM = H.[RPT_SRC_SOURCE_NM], RPT_SRC_STATE = H.[RPT_SRC_STATE], RPT_SRC_CD_DESC = H.[RPT_SRC_CD_DESC], BLD_EXPOSURE_IND = H.[BLD_EXPOSURE_IND], BLD_RECVD_IND = H.[BLD_RECVD_IND], BLD_RECVD_DT = H.[BLD_RECVD_DT], MED_DEN_BLD_CT_FRQ = H.[MED_DEN_BLD_CT_FRQ], MED_DEN_EMPLOYEE_IND = H.[MED_DEN_EMPLOYEE_IND], MED_DEN_EMP_EVER_IND = H.[MED_DEN_EMP_EVER_IND], CLOTFACTOR_PRIOR_1987 = H.[CLOTFACTOR_PRIOR_1987], BLD_CONTAM_IND = H.[BLD_CONTAM_IND], DEN_WORK_OR_SURG_IND = H.[DEN_WORK_OR_SURG_IND], HEMODIALYSIS_IND = H.[HEMODIALYSIS_IND], LT_HEMODIALYSIS_IND = H.[LT_HEMODIALYSIS_IND], HSPTL_PRIOR_ONSET_IND = H.[HSPTL_PRIOR_ONSET_IND], EVER_INJCT_NOPRSC_DRG = H.[EVER_INJCT_NOPRSC_DRG], INCAR_24PLUSHRS_IND = H.[INCAR_24PLUSHRS_IND], INCAR_6PLUS_MO_IND = H.[INCAR_6PLUS_MO_IND], EVER_INCAR_IND = H.[EVER_INCAR_IND], INCAR_TYPE_JAIL_IND = H.[INCAR_TYPE_JAIL_IND], INCAR_TYPE_PRISON_IND = H.[INCAR_TYPE_PRISON_IND], INCAR_TYPE_JUV_IND = H.[INCAR_TYPE_JUV_IND], LAST6PLUSMO_INCAR_PER = H.[LAST6PLUSMO_INCAR_PER], LAST6PLUSMO_INCAR_YR = H.[LAST6PLUSMO_INCAR_YR], OUTPAT_IV_INF_IND = H.[OUTPAT_IV_INF_IND], LTCARE_RESIDENT_IND = H.[LTCARE_RESIDENT_IND], LIFE_SEX_PRTNR_NBR = H.[LIFE_SEX_PRTNR_NBR], BLD_EXPOSURE_OTH = H.[BLD_EXPOSURE_OTH], PIERC_PRIOR_ONSET_INd = H.[PIERC_PRIOR_ONSET_IND], PIERC_PERF_LOC_OTH = H.[PIERC_PERF_LOC_OTH], PIERC_PERF_LOC = H.[PIERC_PERF_LOC], PUB_SAFETY_BLD_CT_FRQ = H.[PUB_SAFETY_BLD_CT_FRQ], PUB_SAFETY_WORKER_IND = H.[PUB_SAFETY_WORKER_IND], STD_TREATED_IND = H.[STD_TREATED_IND], STD_LAST_TREATMENT_YR = H.[STD_LAST_TREATMENT_YR], NON_ORAL_SURGERY_IND = H.[NON_ORAL_SURGERY_IND], TATT_PRIOR_ONSET_IND = H.[TATT_PRIOR_ONSET_IND], TATTOO_PERF_LOC = H.[TATTOO_PERF_LOC], TATT_PRIOR_LOC_OTH = H.[TATT_PRIOR_LOC_OTH], BLD_TRANSF_PRIOR_1992 = H.[BLD_TRANSF_PRIOR_1992], ORGN_TRNSP_PRIOR_1992 = H.[ORGN_TRNSP_PRIOR_1992], TRANSMISSION_MODE = H.[TRANSMISSION_MODE], HOUSEHOLD_TRAVEL_IND = H.[HOUSEHOLD_TRAVEL_IND], TRAVEL_OUT_USACAN_IND = H.[TRAVEL_OUT_USACAN_IND], TRAVEL_OUT_USACAN_LOC = H.[TRAVEL_OUT_USACAN_LOC], HOUSEHOLD_TRAVEL_LOC = H.[HOUSEHOLD_TRAVEL_LOC], TRAVEL_REASON = H.[TRAVEL_REASON], IMM_GLOB_RECVD_IND = H.[IMM_GLOB_RECVD_IND], GLOB_LAST_RECVD_YR = H.[GLOB_LAST_RECVD_YR], VACC_RECVD_IND = H.[VACC_RECVD_IND], VACC_DOSE_NBR_1 = H.[VACC_DOSE_NBR_1], VACC_RECVD_DT_1 = H.[VACC_RECVD_DT_1], VACC_DOSE_NBR_2 = H.[VACC_DOSE_NBR_2], VACC_RECVD_DT_2 = H.[VACC_RECVD_DT_2], VACC_DOSE_NBR_3 = H.[VACC_DOSE_NBR_3], VACC_RECVD_DT_3 = H.[VACC_RECVD_DT_3], VACC_DOSE_NBR_4 = H.[VACC_DOSE_NBR_4], VACC_RECVD_DT_4 = H.[VACC_RECVD_DT_4], VACC_GT_4_IND = H.[VACC_GT_4_IND], VACC_DOSE_RECVD_NBR = H.[VACC_DOSE_RECVD_NBR], VACC_LAST_RECVD_YR = H.[VACC_LAST_RECVD_YR], ANTI_HBSAG_TESTED_IND = H.[ANTI_HBSAG_TESTED_IND], CONDITION_CD = H.[CONDITION_CD], EVENT_DATE = H.[EVENT_DATE], IMPORT_FROM_CITY = H.[IMPORT_FROM_CITY], IMPORT_FROM_COUNTRY = H.[IMPORT_FROM_COUNTRY], IMPORT_FROM_COUNTY = H.[IMPORT_FROM_COUNTY], IMPORT_FROM_STATE = H.[IMPORT_FROM_STATE], INVESTIGATION_KEY = H.[INVESTIGATION_KEY], INVESTIGATOR_NAME = H.[INVESTIGATOR_NAME], PAT_ELECTRONIC_IND = H.[PAT_ELECTRONIC_IND], PHYS_CITY = H.[PHYS_CITY], PHYS_COUNTY = H.[PHYS_COUNTY], PHYS_NAME = H.[PHYS_NAME], PHYS_STATE = H.[PHYS_STATE], PROGRAM_JURISDICTION_OID = H.[PROGRAM_JURISDICTION_OID], RPT_SRC_CITY = H.[RPT_SRC_CITY], RPT_SRC_COUNTY = H.[RPT_SRC_COUNTY], RPT_SRC_COUNTY_CD = H.[RPT_SRC_COUNTY_CD], PHYSICIAN_UID = H.[PHYSICIAN_UID], PATIENT_UID = H.[PATIENT_UID], CASE_UID = H.[CASE_UID], INVESTIGATOR_UID = H.[INVESTIGATOR_UID], REPORTING_SOURCE_UID = H.[REPORTING_SOURCE_UID], REFRESH_DATETIME = H.[REFRESH_DATETIME], PAT_BIRTH_COUNTRY = H.[PAT_BIRTH_COUNTRY]
			FROM dbo.TMP_HEPATITIS_CASE_BASE H WITH(NOLOCK)
			WHERE H.[CASE_UID] = [dbo].[HEPATITIS_DATAMART].[CASE_UID] AND 
				  H.[PATIENT_UID] = [dbo].[HEPATITIS_DATAMART].[PATIENT_UID] AND 
				  H.[INVESTIGATION_KEY] = [dbo].[HEPATITIS_DATAMART].[INVESTIGATION_KEY];
			COMMIT TRANSACTION;

			--------------------------------------29.-----Final ---Inserting into dbo.HEPATITIS_DATAMART----------------------------------------------

			BEGIN TRANSACTION;
			SET @PROC_STEP_NO = 29;
			SET @PROC_STEP_NAME = 'Inserting new entries dbo.HEPATITIS_DATAMART';
			INSERT INTO dbo.[HEPATITIS_DATAMART]( INIT_NND_NOT_DT, ---1
			CASE_RPT_MMWR_WEEK, ---2
			CASE_RPT_MMWR_YEAR, ----3
			HEP_D_INFECTION_IND, ----4
			HEP_MEDS_RECVD_IND, ----5
			HEP_C_TOTAL_ANTIBODY, ----6
			DIAGNOSIS_DT, ------------7
			DIE_FRM_THIS_ILLNESS_IND, ----8
			DISEASE_IMPORTED_IND, -----9
			EARLIEST_RPT_TO_CNTY, ----10
			EARLIEST_RPT_TO_STATE_DT, ---11
			BINATIONAL_RPTNG_CRIT, ---12
			CHILDCARE_CASE_IND, ---13
			CNTRY_USUAL_RESIDENCE, ---14
			CT_BABYSITTER_IND, ---15
			CT_CHILDCARE_IND, ----16
			CT_HOUSEHOLD_IND, ---17
			HEP_CONTACT_IND, ----18
			HEP_CONTACT_EVER_IND, ----19
			OTHER_CONTACT_IND, ---20
			COM_SRC_OUTBREAK_IND, ---21
			CONTACT_TYPE_OTH, ----22
			CT_PLAYMATE_IND, ----23
			SEXUAL_PARTNER_IND, ----24
			DNP_HOUSEHOLD_CT_IND, ---25
			HEP_A_EPLINK_IND, ----25
			FEMALE_SEX_PRTNR_NBR, ---27
			FOODHNDLR_PRIOR_IND, ----28
			DNP_EMPLOYEE_IND, -----29
			STREET_DRUG_INJECTED, -----30
			MALE_SEX_PRTNR_NBR, ----31
			OUTBREAK_IND, -----32
			OBRK_FOODHNDLR_IND, ----33
			FOOD_OBRK_FOOD_ITEM, ----34
			OBRK_NOFOODHNDLR_IND, ----35
			OBRK_UNIDENTIFIED_IND, ----36
			OBRK_WATERBORNE_IND, ----37
			STREET_DRUG_USED, ---38
			SEX_PREF, ----39
			HSPTL_ADMISSION_DT, ----40
			HSPTL_DISCHARGE_DT, ----41
			HSPTL_DURATION_DAYS, ----42
			HSPTLIZD_IND, ---43
			ILLNESS_ONSET_DT, ----44
			INV_CASE_STATUS, ----45
			INV_COMMENTS, ----46
			INV_LOCAL_ID, ---47
			INV_RPT_DT, ---48
			INV_START_DT, ---49
			INVESTIGATION_STATUS, ---50
			JURISDICTION_NM, ---51
			ALT_SGPT_RESULT, ----52
			ANTI_HBS_POS_REAC_IND, -----53
			AST_SGOT_RESULT, ---54
			HEP_E_ANTIGEN, ----55
			HBE_AG_DT, ---56
			HEP_B_SURFACE_ANTIGEN, ----57
			HBS_AG_DT, ----58
			HEP_B_DNA, -----59
			HBV_NAT_DT, ----60	
			HCV_RNA, ----61
			HCV_RNA_DT, ----62
			HEP_D_TEST_IND, ----63
			HEP_A_IGM_ANTIBODY, ----64
			IGM_ANTI_HAV_DT, ----65
			HEP_B_IGM_ANTIBODY, ----66
			IGM_ANTI_HBC_DT, ----67
			PREV_NEG_HEP_TEST_IND, ------68
			ANTIHCV_SIGCUT_RATIO, ----69
			ANTIHCV_SUPP_ASSAY, -----70
			SUPP_ANTI_HCV_DT, -----71
			ALT_RESULT_DT, ----72
			AST_RESULT_DT, -----73
			ALT_SGPT_RSLT_UP_LMT, ----74
			AST_SGOT_RSLT_UP_LMT, ----75
			HEP_A_TOTAL_ANTIBODY, ----76
			TOTAL_ANTI_HAV_DT, ----77
			HEP_B_TOTAL_ANTIBODY, ----78
			TOTAL_ANTI_HBC_DT, ----79
			TOTAL_ANTI_HCV_DT, ----80
			HEP_D_TOTAL_ANTIBODY, ----81
			TOTAL_ANTI_HDV_DT, ----82
			HEP_E_TOTAL_ANTIBODY, ----83
			TOTAL_ANTI_HEV_DT, ----84
			VERIFIED_TEST_DT, ----85
			LEGACY_CASE_ID, ---86
			DIABETES_IND, ----87
			DIABETES_DX_DT, ----88
			PREGNANCY_DUE_DT, ----89
			PAT_JUNDICED_IND, -----90
			PAT_PREV_AWARE_IND, ----91
			HEP_CARE_PROVIDER, ----92
			TEST_REASON, ------93
			TEST_REASON_OTH, -----94
			SYMPTOMATIC_IND, -----95
			MTH_BORN_OUTSIDE_US, ---96
			MTH_ETHNICITY, ------97
			MTH_HBS_AG_PRIOR_POS, ----98
			MTH_POS_AFTER, ------99
			MTH_POS_TEST_DT, ----100
			MTH_RACE, -----101
			MTH_BIRTH_COUNTRY, ----102
			NOT_SUBMIT_DT, ----103
			PAT_REPORTED_AGE, ----104
			PAT_REPORTED_AGE_UNIT, ----105	
			PAT_CITY, ----106
			PAT_COUNTRY, ----107
			PAT_COUNTY, ----108
			PAT_CURR_GENDER, ---109
			PAT_DOB, ---110
			PAT_ETHNICITY, ---111
			PAT_FIRST_NM, ---112
			PAT_LAST_NM, ---113
			PAT_LOCAL_ID, ---114
			PAT_MIDDLE_NM, ---115
			PAT_PREGNANT_IND, ---116
			PAT_RACE, ----117
			PAT_STATE, ----118
			PAT_STREET_ADDR_1, ---119
			PAT_STREET_ADDR_2, ----120	
			PAT_ZIP_CODE, ----121
			RPT_SRC_SOURCE_NM, ----122
			RPT_SRC_STATE, ----123
			RPT_SRC_CD_DESC, ---124
			BLD_EXPOSURE_IND, ----125
			BLD_RECVD_IND, ----126	
			BLD_RECVD_DT, ----127
			MED_DEN_BLD_CT_FRQ, ----128
			MED_DEN_EMP_EVER_IND, ----129
			MED_DEN_EMPLOYEE_IND, ---130
			CLOTFACTOR_PRIOR_1987, ----131	
			BLD_CONTAM_IND, ------132
			DEN_WORK_OR_SURG_IND, ----133
			HEMODIALYSIS_IND, ----134
			LT_HEMODIALYSIS_IND, -----135
			HSPTL_PRIOR_ONSET_IND, ----136
			EVER_INJCT_NOPRSC_DRG, -----137	
			INCAR_24PLUSHRS_IND, ---138
			INCAR_6PLUS_MO_IND, ---139
			EVER_INCAR_IND, ----140
			INCAR_TYPE_JAIL_IND, ----141
			INCAR_TYPE_PRISON_IND, ----142
			INCAR_TYPE_JUV_IND, ----143
			LAST6PLUSMO_INCAR_PER, ----144	
			LAST6PLUSMO_INCAR_YR, ----145
			OUTPAT_IV_INF_IND, ----146
			LTCARE_RESIDENT_IND, ----147
			LIFE_SEX_PRTNR_NBR, ----148
			BLD_EXPOSURE_OTH, ----149
			PIERC_PRIOR_ONSET_IND, ----150	
			PIERC_PERF_LOC_OTH, ----151	
			PIERC_PERF_LOC, -----152
			PUB_SAFETY_BLD_CT_FRQ, ----153	
			PUB_SAFETY_WORKER_IND, ----154	
			STD_TREATED_IND, ----155
			STD_LAST_TREATMENT_YR, ---156	
			NON_ORAL_SURGERY_IND, ---157
			TATT_PRIOR_ONSET_IND, ---158
			TATTOO_PERF_LOC, ----158
			TATT_PRIOR_LOC_OTH, ----160
			BLD_TRANSF_PRIOR_1992, ---161
			ORGN_TRNSP_PRIOR_1992, ----162	
			TRANSMISSION_MODE, ----163
			HOUSEHOLD_TRAVEL_IND, ----164
			TRAVEL_OUT_USACAN_IND, ----165
			TRAVEL_OUT_USACAN_LOC, -----166
			HOUSEHOLD_TRAVEL_LOC, ----167
			TRAVEL_REASON, -----168
			IMM_GLOB_RECVD_IND, ----169
			GLOB_LAST_RECVD_YR, ----170
			VACC_RECVD_IND, ----171
			VACC_DOSE_NBR_1, ----172
			VACC_RECVD_DT_1, ----173
			VACC_DOSE_NBR_2, ----174
			VACC_RECVD_DT_2, ----175
			VACC_DOSE_NBR_3, ----176
			VACC_RECVD_DT_3, -----177
			VACC_DOSE_NBR_4, -----178
			VACC_RECVD_DT_4, ---179
			VACC_GT_4_IND, -----180
			VACC_DOSE_RECVD_NBR, ----181
			VACC_LAST_RECVD_YR, -----182
			ANTI_HBSAG_TESTED_IND, ----183
			CONDITION_CD, -----184
			EVENT_DATE, ----185
			IMPORT_FROM_CITY, ----186
			IMPORT_FROM_COUNTRY, ----187
			IMPORT_FROM_COUNTY, ----188
			IMPORT_FROM_STATE, ----189
			INVESTIGATION_KEY, -----190
			INVESTIGATOR_NAME, -----191
			PAT_ELECTRONIC_IND, ----192
			PHYS_CITY, ----193
			PHYS_COUNTY, ----194
			PHYS_NAME, ----195
			PHYS_STATE, ----196
			PROGRAM_JURISDICTION_OID, ----197
			RPT_SRC_CITY, ---198
			RPT_SRC_COUNTY, ----199
			RPT_SRC_COUNTY_CD, ---200
			PHYSICIAN_UID, ----201
			PATIENT_UID, ----202
			CASE_UID, ---203
			INVESTIGATOR_UID, ---204
			REPORTING_SOURCE_UID, ---205
			REFRESH_DATETIME, ---206
			PAT_BIRTH_COUNTRY ----207
			)
				   SELECT INIT_NND_NOT_DT, ---1
				   CASE_RPT_MMWR_WEEK, ---2
				   CASE_RPT_MMWR_YEAR, ----3
				   HEP_D_INFECTION_IND, ----4
				   HEP_MEDS_RECVD_IND, ----5
				   HEP_C_TOTAL_ANTIBODY, ----6
				   DIAGNOSIS_DT, ------------7
				   DIE_FRM_THIS_ILLNESS_IND, ----8
				   DISEASE_IMPORTED_IND, -----9
				   EARLIEST_RPT_TO_CNTY, ----10
				   EARLIEST_RPT_TO_STATE_DT, ---11
				   BINATIONAL_RPTNG_CRIT, ---12
				   CHILDCARE_CASE_IND, ---13
				   CNTRY_USUAL_RESIDENCE, ---14
				   CT_BABYSITTER_IND, ---15
				   CT_CHILDCARE_IND, ----16
				   CT_HOUSEHOLD_IND, ---17
				   HEP_CONTACT_IND, ----18
				   HEP_CONTACT_EVER_IND, ----19
				   OTHER_CONTACT_IND, ---20
				   COM_SRC_OUTBREAK_IND, ---21
				   CONTACT_TYPE_OTH, ----22
				   CT_PLAYMATE_IND, ----23
				   SEXUAL_PARTNER_IND, ----24
				   DNP_HOUSEHOLD_CT_IND, ---25
				   HEP_A_EPLINK_IND, ----25
				   FEMALE_SEX_PRTNR_NBR, ---27
				   FOODHNDLR_PRIOR_IND, ----28
				   DNP_EMPLOYEE_IND, -----29
				   STREET_DRUG_INJECTED, -----30
				   MALE_SEX_PRTNR_NBR, ----31
				   OUTBREAK_IND, -----32
				   OBRK_FOODHNDLR_IND, ----33
				   FOOD_OBRK_FOOD_ITEM, ----34
				   OBRK_NOFOODHNDLR_IND, ----35
				   OBRK_UNIDENTIFIED_IND, ----36
				   OBRK_WATERBORNE_IND, ----37
				   STREET_DRUG_USED, ---38
				   SEX_PREF, ----39
				   HSPTL_ADMISSION_DT, ----40
				   HSPTL_DISCHARGE_DT, ----41
				   HSPTL_DURATION_DAYS, ----42
				   HSPTLIZD_IND, ---43
				   ILLNESS_ONSET_DT, ----44
				   INV_CASE_STATUS, ----45
				   INV_COMMENTS, ----46
				   INV_LOCAL_ID, ---47
				   INV_RPT_DT, ---48
				   INV_START_DT, ---49
				   INVESTIGATION_STATUS, ---50
				   JURISDICTION_NM, ---51
				   ALT_SGPT_RESULT, ----52
				   ANTI_HBS_POS_REAC_IND, -----53
				   AST_SGOT_RESULT, ---54
				   HEP_E_ANTIGEN, ----55
				   HBE_AG_DT, ---56
				   HEP_B_SURFACE_ANTIGEN, ----57
				   HBS_AG_DT, ----58
				   HEP_B_DNA, -----59
				   HBV_NAT_DT, ----60	
				   HCV_RNA, ----61
				   HCV_RNA_DT, ----62
				   HEP_D_TEST_IND, ----63
				   HEP_A_IGM_ANTIBODY, ----64
				   IGM_ANTI_HAV_DT, ----65
				   HEP_B_IGM_ANTIBODY, ----66
				   IGM_ANTI_HBC_DT, ----67
				   PREV_NEG_HEP_TEST_IND, ------68
				   ANTIHCV_SIGCUT_RATIO, ----69
				   ANTIHCV_SUPP_ASSAY, -----70
				   SUPP_ANTI_HCV_DT, -----71
				   ALT_RESULT_DT, ----72
				   AST_RESULT_DT, -----73
				   ALT_SGPT_RSLT_UP_LMT, ----74
				   AST_SGOT_RSLT_UP_LMT, ----75
				   HEP_A_TOTAL_ANTIBODY, ----76
				   TOTAL_ANTI_HAV_DT, ----77
				   HEP_B_TOTAL_ANTIBODY, ----78
				   TOTAL_ANTI_HBC_DT, ----79
				   TOTAL_ANTI_HCV_DT, ----80
				   HEP_D_TOTAL_ANTIBODY, ----81
				   TOTAL_ANTI_HDV_DT, ----82
				   HEP_E_TOTAL_ANTIBODY, ----83
				   TOTAL_ANTI_HEV_DT, ----84
				   VERIFIED_TEST_DT, ----85
				   LEGACY_CASE_ID, ---86
				   DIABETES_IND, ----87
				   DIABETES_DX_DT, ----88
				   PREGNANCY_DUE_DT, ----89
				   PAT_JUNDICED_IND, -----90
				   PAT_PREV_AWARE_IND, ----91
				   HEP_CARE_PROVIDER, ----92
				   TEST_REASON, ------93
				   TEST_REASON_OTH, -----94
				   SYMPTOMATIC_IND, -----95
				   MTH_BORN_OUTSIDE_US, ---96
				   MTH_ETHNICITY, ------97
				   MTH_HBS_AG_PRIOR_POS, ----98
				   MTH_POS_AFTER, ------99
				   MTH_POS_TEST_DT, ----100
				   MTH_RACE, -----101
				   MTH_BIRTH_COUNTRY, ----102
				   NOT_SUBMIT_DT, ----103
				   PAT_REPORTED_AGE, ----104
				   PAT_REPORTED_AGE_UNIT, ----105	
				   PAT_CITY, ----106
				   PAT_COUNTRY, ----107
				   PAT_COUNTY, ----108
				   PAT_CURR_GENDER, ---109
				   PAT_DOB, ---110
				   PAT_ETHNICITY, ---111
				   PAT_FIRST_NM, ---112
				   PAT_LAST_NM, ---113
				   PAT_LOCAL_ID, ---114
				   PAT_MIDDLE_NM, ---115
				   PAT_PREGNANT_IND, ---116
				   PAT_RACE, ----117
				   PAT_STATE, ----118
				   PAT_STREET_ADDR_1, ---119
				   PAT_STREET_ADDR_2, ----120	
				   PAT_ZIP_CODE, ----121
				   RPT_SRC_SOURCE_NM, ----122
				   RPT_SRC_STATE, ----123
				   RPT_SRC_CD_DESC, ---124
				   BLD_EXPOSURE_IND, ----125
				   BLD_RECVD_IND, ----126	
				   BLD_RECVD_DT, ----127
				   MED_DEN_BLD_CT_FRQ, ----128
				   MED_DEN_EMP_EVER_IND, ----129
				   MED_DEN_EMPLOYEE_IND, ---130
				   CLOTFACTOR_PRIOR_1987, ----131	
				   BLD_CONTAM_IND, ------132
				   DEN_WORK_OR_SURG_IND, ----133
				   HEMODIALYSIS_IND, ----134
				   LT_HEMODIALYSIS_IND, -----135
				   HSPTL_PRIOR_ONSET_IND, ----136
				   EVER_INJCT_NOPRSC_DRG, -----137	
				   INCAR_24PLUSHRS_IND, ---138
				   INCAR_6PLUS_MO_IND, ---139
				   EVER_INCAR_IND, ----140
				   INCAR_TYPE_JAIL_IND, ----141
				   INCAR_TYPE_PRISON_IND, ----142
				   INCAR_TYPE_JUV_IND, ----143
				   LAST6PLUSMO_INCAR_PER, ----144	
				   LAST6PLUSMO_INCAR_YR, ----145
				   OUTPAT_IV_INF_IND, ----146
				   LTCARE_RESIDENT_IND, ----147
				   LIFE_SEX_PRTNR_NBR, ----148
				   BLD_EXPOSURE_OTH, ----149
				   PIERC_PRIOR_ONSET_IND, ----150	
				   PIERC_PERF_LOC_OTH, ----151	
				   PIERC_PERF_LOC, -----152
				   PUB_SAFETY_BLD_CT_FRQ, ----153	
				   PUB_SAFETY_WORKER_IND, ----154	
				   STD_TREATED_IND, ----155
				   STD_LAST_TREATMENT_YR, ---156	
				   NON_ORAL_SURGERY_IND, ---157
				   TATT_PRIOR_ONSET_IND, ---158
				   TATTOO_PERF_LOC, ----158
				   TATT_PRIOR_LOC_OTH, ----160
				   BLD_TRANSF_PRIOR_1992, ---161
				   ORGN_TRNSP_PRIOR_1992, ----162	
				   TRANSMISSION_MODE, ----163
				   HOUSEHOLD_TRAVEL_IND, ----164
				   TRAVEL_OUT_USACAN_IND, ----165
				   TRAVEL_OUT_USACAN_LOC, -----166
				   HOUSEHOLD_TRAVEL_LOC, ----167
				   TRAVEL_REASON, -----168
				   IMM_GLOB_RECVD_IND, ----169
				   GLOB_LAST_RECVD_YR, ----170
				   VACC_RECVD_IND, ----171
				   VACC_DOSE_NBR_1, ----172
				   VACC_RECVD_DT_1, ----173
				   VACC_DOSE_NBR_2, ----174
				   VACC_RECVD_DT_2, ----175
				   VACC_DOSE_NBR_3, ----176
				   VACC_RECVD_DT_3, -----177
				   VACC_DOSE_NBR_4, -----178
				   VACC_RECVD_DT_4, ---179
				   VACC_GT_4_IND, -----180
				   VACC_DOSE_RECVD_NBR, ----181
				   VACC_LAST_RECVD_YR, -----182
				   ANTI_HBSAG_TESTED_IND, ----183
				   CONDITION_CD, -----184
				   EVENT_DATE, ----185
				   IMPORT_FROM_CITY, ----186
				   IMPORT_FROM_COUNTRY, ----187
				   IMPORT_FROM_COUNTY, ----188
				   IMPORT_FROM_STATE, ----189
				   INVESTIGATION_KEY, -----190
				   INVESTIGATOR_NAME, -----191
				   PAT_ELECTRONIC_IND, ----192
				   PHYS_CITY, ----193
				   PHYS_COUNTY, ----194
				   PHYS_NAME, ----195
				   PHYS_STATE, ----196
				   PROGRAM_JURISDICTION_OID, ----197
				   RPT_SRC_CITY, ---198
				   RPT_SRC_COUNTY, ----199
				   RPT_SRC_COUNTY_CD, ---200
				   PHYSICIAN_UID, ----201
				   PATIENT_UID, ----202
				   CASE_UID, ---203
				   INVESTIGATOR_UID, ---204
				   REPORTING_SOURCE_UID, ---205
				   REFRESH_DATETIME, ---206
				   PAT_BIRTH_COUNTRY----207
				   FROM dbo.TMP_HEPATITIS_CASE_BASE AS TH WITH(NOLOCK)
				   WHERE NOT EXISTS
				   (
					   SELECT *
					   FROM [dbo].[HEPATITIS_DATAMART] WITH(NOLOCK)
					   WHERE [CASE_UID] = TH.[CASE_UID] AND 
							 [PATIENT_UID] = TH.[PATIENT_UID] AND 
							 [INVESTIGATION_KEY] = TH.[INVESTIGATION_KEY]
				   )
				   ORDER BY INVESTIGATION_KEY;
			SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			INSERT INTO [DBO].[JOB_FLOW_LOG]( BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT] )
			VALUES( @BATCH_ID, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'START', @PROC_STEP_NO, @PROC_STEP_NAME, @ROWCOUNT_NO );
			COMMIT TRANSACTION;
			---------------------------------------------------------------------------Dropping All TMP Tables------------------------------------------------------------
			IF OBJECT_ID('dbo.HEPATITIS_DATAMART_LAST', 'U') IS NOT NULL
			BEGIN  -------------2 
				DROP TABLE dbo.HEPATITIS_DATAMART_LAST;
			END;
			IF OBJECT_ID('dbo.Update_Patient_Cases', 'U') IS NOT NULL
			BEGIN   -----------3
				DROP TABLE dbo.Update_Patient_Cases;
			END;
			IF OBJECT_ID('dbo.Updated_Hep_Patient', 'U') IS NOT NULL
			BEGIN   -------------4
				DROP TABLE dbo.Updated_Hep_Patient;
			END;
			IF OBJECT_ID('dbo.Updated_Hep_PHYSICIAN', 'U') IS NOT NULL
			BEGIN   -----------5
				DROP TABLE dbo.Updated_Hep_PHYSICIAN;
			END;
			IF OBJECT_ID('dbo.Updated_Hep_INVESTIGATOR', 'U') IS NOT NULL
			BEGIN --------6  
				DROP TABLE dbo.Updated_Hep_INVESTIGATOR;
			END;
			IF OBJECT_ID('dbo.Updated_Hep_REPORTING', 'U') IS NOT NULL
			BEGIN   ---------7
				DROP TABLE dbo.Updated_Hep_REPORTING;
			END;
			IF OBJECT_ID('dbo.EXISTING_HEPATITIS_DATAMART', 'U') IS NOT NULL
			BEGIN   -----8
				DROP TABLE dbo.EXISTING_HEPATITIS_DATAMART;
			END;
			IF OBJECT_ID('dbo.TMP_CONDITION', 'U') IS NOT NULL
			BEGIN   -------------------9
				DROP TABLE dbo.TMP_CONDITION;
			END;
			IF OBJECT_ID('dbo.TMP_F_PAGE_CASE', 'U') IS NOT NULL
			BEGIN  -------------10 
				DROP TABLE dbo.TMP_F_PAGE_CASE;
			END;
			IF OBJECT_ID('dbo.TMP_F_INV_ADMINISTRATIVE', 'U') IS NOT NULL
			BEGIN   -----11
				DROP TABLE dbo.TMP_F_INV_ADMINISTRATIVE;
			END;
			IF OBJECT_ID('dbo.TMP_D_INV_ADMINISTRATIVE', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_INV_ADMINISTRATIVE;
			END;
			IF OBJECT_ID('dbo.TMP_F_INV_CLINICAL', 'U') IS NOT NULL
			BEGIN   ------------12
				DROP TABLE dbo.TMP_F_INV_CLINICAL;
			END;
			IF OBJECT_ID('dbo.TMP_D_INV_CLINICAL', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_INV_CLINICAL;
			END;
			IF OBJECT_ID('dbo.TMP_D_INV_PATIENT_OBS', 'U') IS NOT NULL
			BEGIN   -----------13
				DROP TABLE dbo.TMP_D_INV_PATIENT_OBS;
			END;
			IF OBJECT_ID('dbo.TMP_F_INV_PATIENT_OBS', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_F_INV_PATIENT_OBS;
			END;
			IF OBJECT_ID('dbo.TMP_F_INV_EPIDEMIOLOGY', 'U') IS NOT NULL
			BEGIN   ---------14
				DROP TABLE dbo.TMP_F_INV_EPIDEMIOLOGY;
			END;
			IF OBJECT_ID('dbo.TMP_D_INV_EPIDEMIOLOGY', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_INV_EPIDEMIOLOGY;
			END;
			IF OBJECT_ID('dbo.TMP_F_INV_LAB_FINDING', 'U') IS NOT NULL
			BEGIN   --------15
				DROP TABLE dbo.TMP_F_INV_LAB_FINDING;
			END;
			IF OBJECT_ID('dbo.TMP_D_INV_LAB_FINDING', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_INV_LAB_FINDING;
			END;
			IF OBJECT_ID('dbo.TMP_F_INV_MEDICAL_HISTORY', 'U') IS NOT NULL
			BEGIN   -----16
				DROP TABLE dbo.TMP_F_INV_MEDICAL_HISTORY;
			END;
			IF OBJECT_ID('dbo.TMP_D_INV_MEDICAL_HISTORY', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_INV_MEDICAL_HISTORY;
			END;
			IF OBJECT_ID('dbo.TMP_F_INV_MOTHER', 'U') IS NOT NULL
			BEGIN  -----------17 
				DROP TABLE dbo.TMP_F_INV_MOTHER;
			END;
			IF OBJECT_ID('dbo.TMP_D_INV_MOTHER', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_INV_MOTHER;
			END;
			IF OBJECT_ID('dbo.TMP_F_INV_RISK_FACTOR', 'U') IS NOT NULL
			BEGIN   -----18
				DROP TABLE dbo.TMP_F_INV_RISK_FACTOR;
			END;
			IF OBJECT_ID('dbo.TMP_D_INV_RISK_FACTOR', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_INV_RISK_FACTOR;
			END;
			IF OBJECT_ID('dbo.TMP_F_INV_TRAVEL', 'U') IS NOT NULL
			BEGIN   ----------------19
				DROP TABLE dbo.TMP_F_INV_TRAVEL;
			END;
			IF OBJECT_ID('dbo.TMP_D_INV_TRAVEL', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_INV_TRAVEL;
			END;
			IF OBJECT_ID('dbo.TMP_F_INV_VACCINATION', 'U') IS NOT NULL
			BEGIN   ----20
				DROP TABLE dbo.TMP_F_INV_VACCINATION;
			END;
			IF OBJECT_ID('dbo.TMP_D_INV_VACCINATION', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_INV_VACCINATION;
			END;
			IF OBJECT_ID('dbo.TMP_D_Patient', 'U') IS NOT NULL
			BEGIN  ----21 
				DROP TABLE dbo.TMP_D_Patient;
			END;
			IF OBJECT_ID('dbo.TMP_Investigation', 'U') IS NOT NULL
			BEGIN   ---22
				DROP TABLE TMP_Investigation;
			END;
			IF OBJECT_ID('dbo.TMP_HEP_PAT_PROV', 'U') IS NOT NULL
			BEGIN ----23  
				DROP TABLE TMP_HEP_PAT_PROV;
			END;
			IF OBJECT_ID('dbo.TMP_F_INVESTIGATION_REPEAT', 'U') IS NOT NULL
			BEGIN   -----24
				DROP TABLE dbo.TMP_F_INVESTIGATION_REPEAT;
			END;
			IF OBJECT_ID('dbo.TMP_D_INVESTIGATION_REPEAT', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_D_INVESTIGATION_REPEAT;
			END;
			IF OBJECT_ID('dbo.TMP_METADATA_TEST', 'U') IS NOT NULL
			BEGIN   ---25
				DROP TABLE dbo.TMP_METADATA_TEST;
			END;
			IF OBJECT_ID('dbo.TMP_VAC_REPEAT', 'U') IS NOT NULL
			BEGIN  -----------------26A
				DROP TABLE dbo.TMP_VAC_REPEAT;
			END;
			IF OBJECT_ID('dbo.TMP_VAC_REPEAT_OUT_DATE_Pivot', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_VAC_REPEAT_OUT_DATE_Pivot;
			END;----------------------26b
			IF OBJECT_ID('dbo.TMP_VAC_REPEAT_OUT_DATE', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_VAC_REPEAT_OUT_DATE;
			END;		----------------------26b	
			IF OBJECT_ID('dbo.TMP_VAC_REPEAT_OUT_DATE_Final', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_VAC_REPEAT_OUT_DATE_Final;
			END;----------------------26b

			IF OBJECT_ID('dbo.TMP_VAC_REPEAT_OUT_NUM_Pivot', 'U') IS NOT NULL
			BEGIN   -----26C
				DROP TABLE dbo.TMP_VAC_REPEAT_OUT_NUM_Pivot;
			END;
			IF OBJECT_ID('dbo.TMP_VAC_REPEAT_OUT_NUM', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_VAC_REPEAT_OUT_NUM;
			END;		---------------------------------26C

			IF OBJECT_ID('dbo.TMP_VAC_REPEAT_OUT_NUM_final', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_VAC_REPEAT_OUT_NUM_Final;
			END;	-------------------------------26C

			IF OBJECT_ID('dbo.TMP_VAC_REPEAT_OUT_FINAL1', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_VAC_REPEAT_OUT_FINAL1;
			END;--------------------------------------26D

			IF OBJECT_ID('dbo.TMP_VAC_REPEAT_OUT_FINAL', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.TMP_VAC_REPEAT_OUT_FINAL;
			END;-----------------------------------------------------26e

			IF OBJECT_ID('dbo.TMP_HEPATITIS_CASE_BASE', 'U') IS NOT NULL
			BEGIN    ------27
				DROP TABLE TMP_HEPATITIS_CASE_BASE;
			END; 
			---------------------------------------------------------------------------------------------------------------------------
			BEGIN TRANSACTION;
			SET @Proc_Step_no = 20;
			SET @Proc_Step_Name = 'SP_COMPLETE';
			INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
			VALUES( @batch_id, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'COMPLETE', @Proc_Step_no, @Proc_Step_name, @RowCount_no );
			COMMIT TRANSACTION;
		END	----of if	;
		ELSE
		BEGIN
			PRINT 'No data';
		END;
		COMMIT TRANSACTION;
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
		BEGIN
			ROLLBACK TRANSACTION;
		END;
		DECLARE @ErrorNumber int= ERROR_NUMBER();
		DECLARE @ErrorLine int= ERROR_LINE();
		DECLARE @ErrorMessage nvarchar(4000)= ERROR_MESSAGE();
		DECLARE @ErrorSeverity int= ERROR_SEVERITY();
		DECLARE @ErrorState int= ERROR_STATE();
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [Error_Description], [row_count] )
		VALUES( @Batch_id, 'Hepatitis_Case_DATAMART', 'Hepatitis', 'ERROR', @Proc_Step_no, 'ERROR - ' + @Proc_Step_name, 'Step -' + CAST(@Proc_Step_no AS varchar(3)) + ' -' + CAST(@ErrorMessage AS varchar(500)), 0 );
		RETURN -1;
	END CATCH;
END;---First begin
GO
