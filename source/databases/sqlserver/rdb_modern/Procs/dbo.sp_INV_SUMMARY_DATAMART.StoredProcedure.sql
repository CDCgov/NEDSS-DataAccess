USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[sp_INV_SUMMARY_DATAMART]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[sp_INV_SUMMARY_DATAMART] 

            @batch_id BIGINT
	AS
BEGIN
          
				DECLARE @RowCount_no INT ;
				DECLARE @Proc_Step_no FLOAT = 0 ;
				DECLARE @Proc_Step_Name VARCHAR(200) = '' ;
				DECLARE @batch_start_time datetime2(7) = null ;
				DECLARE @batch_end_time datetime2(7) = null ;
				
				DECLARE @COUNTSTD AS int	
					
				SET @COUNTSTD= (select  count(*) from nbs_changedata.dbo.case_management with(nolock) );
				----Print @COUNTSTD
				
				SELECT @batch_start_time = batch_start_dttm,@batch_end_time = batch_end_dttm
				FROM [dbo].[job_batch_log]
				WHERE status_type = 'start' and type_code='MasterETL';

				DECLARE  @INV_SUMMARY_DATAMART_COUNT AS BIGINT=0;
				SET  @INV_SUMMARY_DATAMART_COUNT= (SELECT COUNT(*) FROM INV_SUMM_DATAMART);
				IF(@INV_SUMMARY_DATAMART_COUNT=0)
				   SET @batch_start_time ='01-01-1990'
				



    BEGIN TRY
			
				SET @Proc_Step_no = 1;
				SET @Proc_Step_Name = 'SP_Start';
			
	
		      BEGIN TRANSACTION;
			
					INSERT INTO [dbo].[job_flow_log] (
							batch_id         ---------------@batch_id
						   ,[Dataflow_Name]  --------------'INV_SUMM_DATAMART'
						   ,[package_Name]   --------------'INV_SUMM_DATAMART'
						   ,[Status_Type]    ---------------START
						   ,[step_number]    ---------------@Proc_Step_no
						   ,[step_name]   ------------------@Proc_Step_Name=sp_start
						   ,[row_count] --------------------0
						   )
						   VALUES
						   (
						   @batch_id
						   ,'INV_SUMM_DATAMART'
						   ,'INV_SUMM_DATAMART'
						   ,'START'
						   ,@Proc_Step_no
						   ,@Proc_Step_Name
						   ,0
						   );
		  
		    COMMIT TRANSACTION;
			
			
				
--------------------------2a. Create Table dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT---STD Cases
if (@COUNTSTD >0) ------------------STD CASES
	
       		
		BEGIN
	---	print 'std'
			BEGIN TRANSACTION;
					SET @Proc_Step_name='Generating TMP_S_PATIENT_LOCATION_KEYS_INIT';
					SET @Proc_Step_no = 2;
						    

									IF OBJECT_ID('dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT', 'U') IS NOT NULL   
 									   drop table dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT;
						
						CREATE TABLE [dbo].TMP_S_PATIENT_LOCATION_KEYS_INIT
									(
									[INVESTIGATION_KEY]           bigint, --1
									INVESTIGATION_STATUS	      varchar(50),---2
									INVESTIGATION_LOCAL_ID	      varchar(50),  ---3 
									EARLIEST_RPT_TO_CNTY_DT       datetime,--4
									EARLIEST_RPT_TO_STATE_DT      datetime, --5
									DIAGNOSIS_DATE	              datetime,---6
									ILLNESS_ONSET_DATE            datetime,  ---7        
									CASE_STATUS                   varchar(50), --8 
									MMWR_WEEK                     Numeric(18,0), ---9                  
									MMWR_YEAR                     Numeric(18,0), ---10                    
									PROGRAM_JURISDICTION_OID      [bigint] , ---11
									HSPTL_ADMISSION_DT            datetime, ---12
									INV_START_DT                  datetime, ---13
									INV_RPT_DT                    datetime, ---14
									CURR_PROCESS_STATE            varchar(100), ---15
									JURISDICTION_NM               varchar(100), ---16
									INVESTIGATION_CREATE_DATE	  datetime, ---17
									INVESTIGATION_CREATED_BY	  bigint, --18
									INVESTIGATION_LAST_UPDTD_DATE datetime, ---19
									INVESTIGATION_LAST_UPDTD_BY	  bigint, ---20
									PROGRAM_AREA			      varchar(50),  ---21 
									GENERIC_PHYSICIAN_KEY         bigint ,---22
									GEN_PATI_KEY                  bigint, --23
									CRS_PHYSICIAN_KEY		      bigint, ---24
									CRS_PAT_KEY			          bigint,  --25
									MEASLES_PHYSICIAN_KEY		  bigint, --26
									MEASLES_PAT_KEY			      bigint,  ---27
									RUBELLA_PHYSICIAN_KEY		  bigint, ---28
									RUBELLA_PAT_KEY			      bigint, ---29
									HEPATITIS_PHYSICIAN_KEY		  bigint,---30
									HEPATITIS_PAT_KEY			  bigint, ---31
									BMIRD_PHYSICIAN_KEY			  bigint , ---32
									BMIRD_PAT_KEY			      bigint, --33
									FIRST_POSITIVE_CULTURE_DT     datetime,---34
									PERTUSSIS_PHYSICIAN_KEY		  bigint,---35
									PERTUSSIS_PAT_KEY			  bigint,---36
									F_TB_PAM_PHYSICIAN_KEY		  bigint,---37
									F_TB_PAM_PAT_KEY			  bigint,---38
									F_VAR_PAM_PHYSICIAN_KEY	      bigint,---39
									F_VAR_PAM_PAT_KEY			  bigint,---40
									F_PAGE_CASE_PHYSICIAN_KEY	  bigint,---41
									F_PAGE_PATIENT_KEY			  bigint,---42
									F_STD_PHYSICIAN_KEY			  bigint,--43
									F_STD_PATIENT_KEY			  bigint, ---STD---44
                                    PATIENT_KEY			          bigint,--added---45
									PHYSICIAN_KEY                 bigint----added---46
									)


									INSERT INTO [dbo].TMP_S_PATIENT_LOCATION_KEYS_INIT
									SELECT DISTINCT
									I.INVESTIGATION_KEY					 AS  'INVESTIGATION_KEY',---1
									I.INVESTIGATION_STATUS				 AS  'INVESTIGATION_STATUS',---2
									I.INV_LOCAL_ID                       AS  'INVESTIGATION_LOCAL_ID',---3
									I.EARLIEST_RPT_TO_CNTY_DT            AS  'EARLIEST_RPT_TO_CNTY_DT',---4
									I.EARLIEST_RPT_TO_STATE_DT           AS  'EARLIEST_RPT_TO_STATE_DT',---5
									I.DIAGNOSIS_DT                       AS  'DIAGNOSIS_DATE',---6
									I.ILLNESS_ONSET_DT                   AS  'ILLNESS_ONSET_DATE',   ---7       
									I.INV_CASE_STATUS                    AS  'CASE_STATUS',---8
									I.CASE_RPT_MMWR_WK                   AS  'MMWR_WEEK',      ---9             
									I.CASE_RPT_MMWR_YR                   AS  'MMWR_YEAR' ,     ---10             
									I.CASE_OID                           AS  'PROGRAM_JURISDICTION_OID',---11
									I.HSPTL_ADMISSION_DT                 AS  'HSPTL_ADMISSION_DT',--12
									I.INV_START_DT                       AS  'INV_START_DT',---13
									I.INV_RPT_DT                         AS  'INV_RPT_DT', ---14
									I.CURR_PROCESS_STATE                 AS  'CURR_PROCESS_STATE',----15
									I.JURISDICTION_NM,                                         ---16
									EM.ADD_TIME							 AS  'INVESTIGATION_CREATE_DATE',--17
									EM.ADD_USER_ID						 AS  'INVESTIGATION_CREATED_BY',---18
									EM.LAST_CHG_TIME					 AS  'INVESTIGATION_LAST_UPDTD_DATE',---19
									EM.LAST_CHG_USER_ID					 AS  'INVESTIGATION_LAST_UPDTD_BY',---20
									EM.PROG_AREA_DESC_TXT				 AS  'PROGRAM_AREA', ---21
								   COALESCE(GC.PHYSICIAN_KEY,0)          AS  'GENERIC_PHYSICIAN_KEY'  ,---22
								   COALESCE(GC.PATIENT_KEY,0)	         AS  'GEN_PATI_KEY',---23
								   COALESCE(CC.PHYSICIAN_KEY,0)			 AS  'CRS_PHYSICIAN_KEY',---24
								   COALESCE(CC.PATIENT_KEY,0)			 AS  'CRS_PAT_KEY',---25
								   COALESCE(MC.PHYSICIAN_KEY,0)			 AS  'MEASLES_PHYSICIAN_KEY',---26
								   COALESCE(MC.PATIENT_KEY,0)			 AS  'MEASLES_PAT_KEY',---27
								   COALESCE(RC.PHYSICIAN_KEY,0)			 AS  'RUBELLA_PHYSICIAN_KEY',---28
								   COALESCE(RC.PATIENT_KEY,0)			 AS  'RUBELLA_PAT_KEY',---29
								   COALESCE(HC.PHYSICIAN_KEY,0)			 AS  'HEPATITIS_PHYSICIAN_KEY',---30
								   COALESCE(HC.PATIENT_KEY,0)		     AS  'HEPATITIS_PAT_KEY',---31
								   COALESCE(BC.PHYSICIAN_KEY,0)		     AS  'BMIRD_PHYSICIAN_KEY',---32
								   COALESCE(BC.PATIENT_KEY,0)			 AS  'BMIRD_PAT_KEY',---33
									BC.FIRST_POSITIVE_CULTURE_DT		 AS  'FIRST_POSITIVE_CULTURE_DT',---34
									COALESCE(PC.PHYSICIAN_KEY,0)		 AS  'PERTUSSIS_PHYSICIAN_KEY',---35
									COALESCE(PC.PATIENT_KEY	,0)			 AS  'PERTUSSIS_PAT_KEY',---36
								    COALESCE(F_TB.PHYSICIAN_KEY	,0)		 AS  'F_TB_PAM_PHYSICIAN_KEY',---37
									COALESCE(F_TB.PERSON_KEY,0)			 AS  'F_TB_PAM_PAT_KEY',---38
									COALESCE(F_VAR.PHYSICIAN_KEY,0)		 AS  'F_VAR_PAM_PHYSICIAN_KEY',---39
								    COALESCE(F_VAR.PERSON_KEY,0)		 AS  'F_VAR_PAM_PAT_KEY',---40
									COALESCE(F_PAGE.PHYSICIAN_KEY,0)	 AS  'F_PAGE_CASE_PHYSICIAN_KEY', ---41
									COALESCE(F_PAGE.PATIENT_KEY	,0)		 AS  'F_PAGE_PATIENT_KEY', ---42
									COALESCE(F_STD.PHYSICIAN_KEY,0)      AS  'F_STD_PHYSICIAN_KEY',---43
									COALESCE(F_STD.PATIENT_KEY,0)		 AS  'F_STD_PATIENT_KEY', ---STD---44
									Cast (NULL as  bigint)				 AS  'PATIENT_KEY',---45
									Cast (NULL as  bigint)               AS  'PHYSICIAN_KEY'---46
									
									FROM [dbo].[INVESTIGATION] I             with (nolock)     
									FULL JOIN  [dbo].GENERIC_CASE    GC      with (nolock) ON GC.INVESTIGATION_KEY    = I.INVESTIGATION_KEY
									FULL JOIN  [dbo].CRS_CASE        CC      with (nolock) ON CC.INVESTIGATION_KEY    = I.INVESTIGATION_KEY
									FULL JOIN  [dbo].MEASLES_CASE    MC      with (nolock) ON MC.INVESTIGATION_KEY    = I.INVESTIGATION_KEY
									FULL JOIN  [dbo].RUBELLA_CASE    RC      with (nolock) ON RC.INVESTIGATION_KEY    = I.INVESTIGATION_KEY
									FULL JOIN  [dbo].HEPATITIS_CASE  HC      with (nolock) ON HC.INVESTIGATION_KEY    = I.INVESTIGATION_KEY
									FULL JOIN  [dbo].BMIRD_CASE      BC      with (nolock) ON BC.INVESTIGATION_KEY    = I.INVESTIGATION_KEY
									FULL JOIN  [dbo].PERTUSSIS_CASE  PC      with (nolock) ON PC.INVESTIGATION_KEY    = I.INVESTIGATION_KEY
									FULL JOIN  [dbo].F_TB_PAM        F_TB    with (nolock) ON F_TB.INVESTIGATION_KEY  = I.INVESTIGATION_KEY
									FULL JOIN  [dbo].F_VAR_PAM       F_VAR   with (nolock) ON F_VAR.INVESTIGATION_KEY = I.INVESTIGATION_KEY
									FULL JOIN  [dbo].F_PAGE_CASE     F_PAGE  with (nolock) ON F_PAGE.INVESTIGATION_KEY= I.INVESTIGATION_KEY
									FULL JOIN  [dbo].[F_STD_PAGE_CASE]F_STD  with (nolock) ON F_STD.INVESTIGATION_KEY = I.INVESTIGATION_KEY--STD
									FULL JOIN  [dbo].EVENT_METRIC    EM      with (nolock) ON EM.LOCAL_ID             = I.INV_LOCAL_ID
									WHERE I.CASE_TYPE= 'I' AND I.RECORD_STATUS_CD = 'ACTIVE'
									  AND I.[LAST_CHG_TIME]  >= @batch_start_time AND I.[LAST_CHG_TIME]<  @batch_end_time;  ---added on 4/5/2021
									 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

												INSERT INTO [DBO].[JOB_FLOW_LOG] 
												(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
												VALUES(@BATCH_ID,'INV_SUMM_DATAMART','INV_SUMM_DATAMART','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  
				
			
							 
										   UPDATE  dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT
										   SET PATIENT_KEY  = 
										   (CASE 
										   WHEN   GEN_PATI_KEY       >1  then  GEN_PATI_KEY 
										   WHEN   CRS_PAT_KEY        >1  then  CRS_PAT_KEY
										   WHEN   MEASLES_PAT_KEY    >1  then  MEASLES_PAT_KEY
										   WHEN   RUBELLA_PAT_KEY    >1  then  RUBELLA_PAT_KEY
										   WHEN   HEPATITIS_PAT_KEY  >1  then  HEPATITIS_PAT_KEY
										   WHEN   BMIRD_PAT_KEY      >1  then  BMIRD_PAT_KEY
										   WHEN   PERTUSSIS_PAT_KEY  >1  then  PERTUSSIS_PAT_KEY
										   WHEN   F_TB_PAM_PAT_KEY   >1  then  F_TB_PAM_PAT_KEY
										   WHEN   F_PAGE_PATIENT_KEY >1  then  F_PAGE_PATIENT_KEY
										   WHEN   F_VAR_PAM_PAT_KEY  >1  then  F_VAR_PAM_PAT_KEY
										   WHEN   F_STD_PATIENT_KEY  >1  then  F_STD_PATIENT_KEY---STD
											ELSE NULL 
										  END
										  )

											UPDATE  dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT
										   SET PHYSICIAN_KEY   = 
										   (CASE 
										   WHEN   GENERIC_PHYSICIAN_KEY    >1  then GENERIC_PHYSICIAN_KEY
										   WHEN   CRS_PHYSICIAN_KEY        >1  then CRS_PHYSICIAN_KEY
										   WHEN   MEASLES_PHYSICIAN_KEY    >1  then MEASLES_PHYSICIAN_KEY
										   WHEN   RUBELLA_PHYSICIAN_KEY    >1  then RUBELLA_PHYSICIAN_KEY
										   WHEN   HEPATITIS_PHYSICIAN_KEY  >1  then HEPATITIS_PHYSICIAN_KEY
										   WHEN   BMIRD_PHYSICIAN_KEY      >1  then BMIRD_PHYSICIAN_KEY
										   WHEN   PERTUSSIS_PHYSICIAN_KEY  >1  then PERTUSSIS_PHYSICIAN_KEY
										   WHEN   F_TB_PAM_PHYSICIAN_KEY   >1  then F_TB_PAM_PHYSICIAN_KEY
										   WHEN   F_VAR_PAM_PHYSICIAN_KEY  >1  then F_VAR_PAM_PHYSICIAN_KEY
										   WHEN   F_PAGE_CASE_PHYSICIAN_KEY>1  then F_PAGE_CASE_PHYSICIAN_KEY
										   WHEN   F_STD_PHYSICIAN_KEY      >1  then F_STD_PHYSICIAN_KEY 
											ELSE NULL 
										  END
										  )


							  
							   
					COMMIT TRANSACTION
			END
	
	---------------------------------------------------2b. Create Table dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT---NON-STD Cases
Else 

        BEGIN
		----print'nonSTd'
		              BEGIN TRANSACTION
					  SET @Proc_Step_name='Generating TMP_S_PATIENT_LOCATION_KEYS_INIT';
					  SET @Proc_Step_no = 2;
						

									IF OBJECT_ID('dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT', 'U') IS NOT NULL   
 									   drop table dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT;
		
	                         	CREATE TABLE [dbo].TMP_S_PATIENT_LOCATION_KEYS_INIT
									(
									[INVESTIGATION_KEY]           bigint, --1
									INVESTIGATION_STATUS	      varchar(50),---2
									INVESTIGATION_LOCAL_ID	      varchar(50),  ---3 
									EARLIEST_RPT_TO_CNTY_DT       datetime,--4
									EARLIEST_RPT_TO_STATE_DT      datetime, --5
									DIAGNOSIS_DATE	              datetime,---6
									ILLNESS_ONSET_DATE            datetime,  ---7        
									CASE_STATUS                   varchar(50), --8 
									MMWR_WEEK                     Numeric(18,0), ---9                  
									MMWR_YEAR                     Numeric(18,0), ---10                    
									PROGRAM_JURISDICTION_OID      [bigint] , ---11
									HSPTL_ADMISSION_DT            datetime, ---12
									INV_START_DT                  datetime, ---13
									INV_RPT_DT                    datetime, ---14
									CURR_PROCESS_STATE            varchar(100), ---15
									JURISDICTION_NM               varchar(100), ---16
									INVESTIGATION_CREATE_DATE	  datetime, ---17
									INVESTIGATION_CREATED_BY	  bigint, --18
									INVESTIGATION_LAST_UPDTD_DATE datetime, ---19
									INVESTIGATION_LAST_UPDTD_BY	  bigint, ---20
									PROGRAM_AREA			      varchar(50),  ---21 
									GENERIC_PHYSICIAN_KEY         bigint ,---22
									GEN_PATI_KEY                  bigint, --23
									CRS_PHYSICIAN_KEY		      bigint, ---24
									CRS_PAT_KEY			          bigint,  --25
									MEASLES_PHYSICIAN_KEY		  bigint, --26
									MEASLES_PAT_KEY			      bigint,  ---27
									RUBELLA_PHYSICIAN_KEY		  bigint, ---28
									RUBELLA_PAT_KEY			      bigint, ---29
									HEPATITIS_PHYSICIAN_KEY		  bigint,---30
									HEPATITIS_PAT_KEY			  bigint, ---31
									BMIRD_PHYSICIAN_KEY			  bigint , ---32
									BMIRD_PAT_KEY			      bigint, --33
									FIRST_POSITIVE_CULTURE_DT     datetime,---34
									PERTUSSIS_PHYSICIAN_KEY		  bigint,---35
									PERTUSSIS_PAT_KEY			  bigint,---36
									F_TB_PAM_PHYSICIAN_KEY		  bigint,---37
									F_TB_PAM_PAT_KEY			  bigint,---38
									F_VAR_PAM_PHYSICIAN_KEY	      bigint,---39
									F_VAR_PAM_PAT_KEY			  bigint,---40
									F_PAGE_CASE_PHYSICIAN_KEY	  bigint,---41
									F_PAGE_PATIENT_KEY			  bigint,---42
								    F_STD_PHYSICIAN_KEY			  bigint,--
									F_STD_PATIENT_KEY			  bigint, ---STD
                                    PATIENT_KEY			          bigint,--added---43
									PHYSICIAN_KEY                 bigint----added---44
									
									)


									INSERT INTO [dbo].TMP_S_PATIENT_LOCATION_KEYS_INIT
									SELECT DISTINCT
									I.INVESTIGATION_KEY					 AS  'INVESTIGATION_KEY',---1
									I.INVESTIGATION_STATUS				 AS  'INVESTIGATION_STATUS',---2
									I.INV_LOCAL_ID                       AS  'INVESTIGATION_LOCAL_ID',---3
									I.EARLIEST_RPT_TO_CNTY_DT            AS  'EARLIEST_RPT_TO_CNTY_DT',---4
									I.EARLIEST_RPT_TO_STATE_DT           AS  'EARLIEST_RPT_TO_STATE_DT',---5
									I.DIAGNOSIS_DT                       AS  'DIAGNOSIS_DATE',---6
									I.ILLNESS_ONSET_DT                   AS  'ILLNESS_ONSET_DATE',   ---7       
									I.INV_CASE_STATUS                    AS  'CASE_STATUS',---8
									I.CASE_RPT_MMWR_WK                   AS  'MMWR_WEEK',      ---9             
									I.CASE_RPT_MMWR_YR                   AS  'MMWR_YEAR' ,     ---10             
									I.CASE_OID                           AS  'PROGRAM_JURISDICTION_OID',---11
									I.HSPTL_ADMISSION_DT                 AS  'HSPTL_ADMISSION_DT',--12
									I.INV_START_DT                       AS  'INV_START_DT',---13
									I.INV_RPT_DT                         AS  'INV_RPT_DT', ---14
									I.CURR_PROCESS_STATE                 AS  'CURR_PROCESS_STATE',----15
									I.JURISDICTION_NM,                                         ---16
									EM.ADD_TIME							 AS  'INVESTIGATION_CREATE_DATE',--17
									EM.ADD_USER_ID						 AS  'INVESTIGATION_CREATED_BY',---18
									EM.LAST_CHG_TIME					 AS  'INVESTIGATION_LAST_UPDTD_DATE',---19
									EM.LAST_CHG_USER_ID					 AS  'INVESTIGATION_LAST_UPDTD_BY',---20
									EM.PROG_AREA_DESC_TXT				 AS  'PROGRAM_AREA', ---21
								   COALESCE(GC.PHYSICIAN_KEY,0)          AS  'GENERIC_PHYSICIAN_KEY'  ,---22
								   COALESCE(GC.PATIENT_KEY,0)	         AS  'GEN_PATI_KEY',---23
								   COALESCE(CC.PHYSICIAN_KEY,0)			 AS  'CRS_PHYSICIAN_KEY',---24
								   COALESCE(CC.PATIENT_KEY,0)			 AS  'CRS_PAT_KEY',---25
								   COALESCE(MC.PHYSICIAN_KEY,0)			 AS  'MEASLES_PHYSICIAN_KEY',---26
								   COALESCE(MC.PATIENT_KEY,0)			 AS  'MEASLES_PAT_KEY',---27
								   COALESCE(RC.PHYSICIAN_KEY,0)			 AS  'RUBELLA_PHYSICIAN_KEY',---28
								   COALESCE(RC.PATIENT_KEY,0)			 AS  'RUBELLA_PAT_KEY',---29
								   COALESCE(HC.PHYSICIAN_KEY,0)			 AS  'HEPATITIS_PHYSICIAN_KEY',---30
								   COALESCE(HC.PATIENT_KEY,0)		     AS  'HEPATITIS_PAT_KEY',---31
								   COALESCE(BC.PHYSICIAN_KEY,0)		     AS  'BMIRD_PHYSICIAN_KEY',---32
								   COALESCE(BC.PATIENT_KEY,0)			 AS  'BMIRD_PAT_KEY',---33
									BC.FIRST_POSITIVE_CULTURE_DT		 AS  'FIRST_POSITIVE_CULTURE_DT',---34
									COALESCE(PC.PHYSICIAN_KEY,0)		 AS  'PERTUSSIS_PHYSICIAN_KEY',---35
									COALESCE(PC.PATIENT_KEY	,0)			 AS  'PERTUSSIS_PAT_KEY',---36
								    COALESCE(F_TB.PHYSICIAN_KEY	,0)		 AS  'F_TB_PAM_PHYSICIAN_KEY',---37
									COALESCE(F_TB.PERSON_KEY,0)			 AS  'F_TB_PAM_PAT_KEY',---38
									COALESCE(F_VAR.PHYSICIAN_KEY,0)		 AS  'F_VAR_PAM_PHYSICIAN_KEY',---39
								    COALESCE(F_VAR.PERSON_KEY,0)		 AS  'F_VAR_PAM_PAT_KEY',---40
									COALESCE(F_PAGE.PHYSICIAN_KEY,0)	 AS  'F_PAGE_CASE_PHYSICIAN_KEY',---41
									COALESCE(F_PAGE.PATIENT_KEY	,0)		 AS  'F_PAGE_PATIENT_KEY',---42
								---	COALESCE(F_STD_PHYSICIAN_KEY,0)	     AS  'F_STD_PHYSICIAN_KEY ---STD
									NULL								 AS  'F_STD_PHYSICIAN_KEY', ---STD	---43
								---	COALESCE(F_STD.PATIENT_KEY,0)		 AS  'F_STD_PATIENT_KEY', ---STD
								    NULL								 AS  'F_STD_PATIENT_KEY',---44
								   Cast (NULL as  bigint)				 AS  'PATIENT_KEY',---45
									Cast (NULL as  bigint)               AS  'PHYSICIAN_KEY'---46
									
									FROM [dbo].[INVESTIGATION] I             with (nolock)     
									FULL JOIN  [dbo].GENERIC_CASE    GC      with (nolock) ON GC.INVESTIGATION_KEY    = I.INVESTIGATION_KEY
									FULL JOIN  [dbo].CRS_CASE        CC      with (nolock) ON CC.INVESTIGATION_KEY    = I.INVESTIGATION_KEY
									FULL JOIN  [dbo].MEASLES_CASE    MC      with (nolock) ON MC.INVESTIGATION_KEY    = I.INVESTIGATION_KEY
									FULL JOIN  [dbo].RUBELLA_CASE    RC      with (nolock) ON RC.INVESTIGATION_KEY    = I.INVESTIGATION_KEY
									FULL JOIN  [dbo].HEPATITIS_CASE  HC      with (nolock) ON HC.INVESTIGATION_KEY    = I.INVESTIGATION_KEY
									FULL JOIN  [dbo].BMIRD_CASE      BC      with (nolock) ON BC.INVESTIGATION_KEY    = I.INVESTIGATION_KEY
									FULL JOIN  [dbo].PERTUSSIS_CASE  PC      with (nolock) ON PC.INVESTIGATION_KEY    = I.INVESTIGATION_KEY
									FULL JOIN  [dbo].F_TB_PAM        F_TB    with (nolock) ON F_TB.INVESTIGATION_KEY  = I.INVESTIGATION_KEY
									FULL JOIN  [dbo].F_VAR_PAM       F_VAR   with (nolock) ON F_VAR.INVESTIGATION_KEY = I.INVESTIGATION_KEY
									FULL JOIN  [dbo].F_PAGE_CASE     F_PAGE  with (nolock) ON F_PAGE.INVESTIGATION_KEY= I.INVESTIGATION_KEY
							---		FULL JOIN  [dbo].[F_STD_PAGE_CASE]F_STD  with (nolock) ON F_STD.INVESTIGATION_KEY = I.INVESTIGATION_KEY--STD
									FULL JOIN  [dbo].EVENT_METRIC    EM      with (nolock) ON EM.LOCAL_ID             = I.INV_LOCAL_ID
									WHERE I.CASE_TYPE= 'I' AND I.RECORD_STATUS_CD = 'ACTIVE'
									  AND I.[LAST_CHG_TIME]  >= @batch_start_time AND I.[LAST_CHG_TIME]<  @batch_end_time;  ---added on 4/5/2021
									
									 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

												INSERT INTO [DBO].[JOB_FLOW_LOG] 
												(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
												VALUES(@BATCH_ID,'INV_SUMM_DATAMART','INV_SUMM_DATAMART','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  
				

								  UPDATE  dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT
							   SET PATIENT_KEY  = 
							   (CASE 
							   WHEN   GEN_PATI_KEY       >1  then  GEN_PATI_KEY 
							   WHEN   CRS_PAT_KEY        >1  then  CRS_PAT_KEY
							   WHEN   MEASLES_PAT_KEY    >1  then  MEASLES_PAT_KEY
							   WHEN   RUBELLA_PAT_KEY    >1  then  RUBELLA_PAT_KEY
                               WHEN   HEPATITIS_PAT_KEY  >1  then  HEPATITIS_PAT_KEY
							   WHEN   BMIRD_PAT_KEY      >1  then  BMIRD_PAT_KEY
							   WHEN   PERTUSSIS_PAT_KEY  >1  then  PERTUSSIS_PAT_KEY
							   WHEN   F_TB_PAM_PAT_KEY   >1  then  F_TB_PAM_PAT_KEY
							   WHEN   F_PAGE_PATIENT_KEY >1  then  F_PAGE_PATIENT_KEY
							   WHEN   F_VAR_PAM_PAT_KEY  >1  then  F_VAR_PAM_PAT_KEY
							   ELSE NULL 
							  END
							  )

							   UPDATE  dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT
							   SET PHYSICIAN_KEY   = 
							   (CASE 
							   WHEN   GENERIC_PHYSICIAN_KEY    >1  then GENERIC_PHYSICIAN_KEY
							   WHEN   CRS_PHYSICIAN_KEY        >1  then CRS_PHYSICIAN_KEY
							   WHEN   MEASLES_PHYSICIAN_KEY    >1  then MEASLES_PHYSICIAN_KEY
							   WHEN   RUBELLA_PHYSICIAN_KEY    >1  then RUBELLA_PHYSICIAN_KEY
                               WHEN   HEPATITIS_PHYSICIAN_KEY  >1  then HEPATITIS_PHYSICIAN_KEY
							   WHEN   BMIRD_PHYSICIAN_KEY      >1  then BMIRD_PHYSICIAN_KEY
							   WHEN   PERTUSSIS_PHYSICIAN_KEY  >1  then PERTUSSIS_PHYSICIAN_KEY
							   WHEN   F_TB_PAM_PHYSICIAN_KEY   >1  then F_TB_PAM_PHYSICIAN_KEY
							   WHEN   F_VAR_PAM_PHYSICIAN_KEY  >1  then F_VAR_PAM_PHYSICIAN_KEY
							   WHEN   F_PAGE_CASE_PHYSICIAN_KEY>1  then F_PAGE_CASE_PHYSICIAN_KEY
						    ---WHEN   F_STD_PHYSICIAN_KEY      >1  then F_STD_PHYSICIAN_KEY 
							    ELSE NULL 
							  END
							  )

		
                         COMMIT TRANSACTION;
		
                     END
				

				                    ALTER TABLE dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT    DROP Column GENERIC_PHYSICIAN_KEY;
									ALTER TABLE dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT	DROP Column CRS_PHYSICIAN_KEY;
									ALTER TABLE dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT	DROP Column MEASLES_PHYSICIAN_KEY;
									ALTER TABLE dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT	DROP Column RUBELLA_PHYSICIAN_KEY;
									ALTER TABLE dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT	DROP Column HEPATITIS_PHYSICIAN_KEY;
									ALTER TABLE dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT	DROP Column BMIRD_PHYSICIAN_KEY;
									ALTER TABLE dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT	DROP Column PERTUSSIS_PHYSICIAN_KEY;
									ALTER TABLE dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT	DROP Column F_TB_PAM_PHYSICIAN_KEY;
									ALTER TABLE dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT	DROP Column F_VAR_PAM_PHYSICIAN_KEY;
									ALTER TABLE dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT	DROP Column F_PAGE_CASE_PHYSICIAN_KEY;
									ALTER TABLE dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT	DROP Column GEN_PATI_KEY;
									ALTER TABLE dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT	DROP Column CRS_PAT_KEY;
									ALTER TABLE dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT	DROP Column MEASLES_PAT_KEY;
									ALTER TABLE dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT	DROP Column RUBELLA_PAT_KEY;
									ALTER TABLE dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT	DROP Column HEPATITIS_PAT_KEY;
									ALTER TABLE dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT	DROP Column BMIRD_PAT_KEY;
									ALTER TABLE dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT	DROP Column PERTUSSIS_PAT_KEY;
									ALTER TABLE dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT	DROP Column F_TB_PAM_PAT_KEY;
									ALTER TABLE dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT	DROP Column F_VAR_PAM_PAT_KEY;
									ALTER TABLE dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT	DROP Column F_STD_PATIENT_KEY ;	
		
		
---------------------------------------------------------3.Create table dbo.TMP_S_CONFIRMATION_METHOD_BASE
				

			BEGIN TRANSACTION

					   SET @PROC_STEP_NO = 3;
					   SET @PROC_STEP_NAME = ' GENERATING TMP_S_CONFIRMATION_METHOD_BASE'; 

						IF OBJECT_ID('dbo.TMP_S_CONFIRMATION_METHOD_BASE', 'U') IS NOT NULL   
						drop table dbo.TMP_S_CONFIRMATION_METHOD_BASE  ;


						SELECT CM.*, 
						       CMG.INVESTIGATION_KEY, 
							   CMG.[CONFIRMATION_DT] 
						INTO  dbo.TMP_S_CONFIRMATION_METHOD_BASE
						FROM 
								 dbo.[CONFIRMATION_METHOD] CM with (nolock),
								 dbo.[CONFIRMATION_METHOD_GROUP]CMG with (nolock), 
							     dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT PL with (nolock)
					
						WHERE CMG.[CONFIRMATION_METHOD_KEY]= CM.[CONFIRMATION_METHOD_KEY]
						  and CMG.[INVESTIGATION_KEY]      = PL.INVESTIGATION_KEY
						ORDER BY CMG.INVESTIGATION_KEY;


						SELECT @ROWCOUNT_NO = @@ROWCOUNT;

									INSERT INTO [DBO].[JOB_FLOW_LOG] 
									(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
									VALUES(@BATCH_ID,'INV_SUMM_DATAMART','INV_SUMM_DATAMART','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		--------------------------------------4. Create Table TMP_S_CONFIRMATION_METHOD_PIVOT

			BEGIN TRANSACTION;
						SET @Proc_Step_no = 4;
						SET @Proc_Step_Name = ' Generating TMP_S_CONFIRMATION_METHOD_PIVOT';
						IF OBJECT_ID('dbo.TMP_S_CONFIRMATION_METHOD_PIVOT', 'U') IS NOT NULL
						DROP TABLE dbo.TMP_S_CONFIRMATION_METHOD_PIVOT;
			


							Declare @CONFIRMATION_METHOD_DESC nvarchar(max)='',
									@sqlQuery nvarchar(max)=''

							;With CTE_Description 
							AS
							(SELECT DISTINCT COALESCE(CONFIRMATION_METHOD_DESC,'NULL') as CMD FROM [dbo].[CONFIRMATION_METHOD] )

							SELECT @CONFIRMATION_METHOD_DESC= @CONFIRMATION_METHOD_DESC+QUOTENAME(LTRIM(RTRIM([CMD]))) +',' from CTE_Description
							SET @CONFIRMATION_METHOD_DESC= LEFT( @CONFIRMATION_METHOD_DESC,len( @CONFIRMATION_METHOD_DESC)-1)

							----Print @CONFIRMATION_METHOD_DESC

							SET @sqlQuery = 
							'
							SELECT * Into dbo.TMP_S_CONFIRMATION_METHOD_PIVOT  FROM 
							(
							SELECT * from
								  (
									 SELECT CONFIRMATION_METHOD_DESC,investigation_key ,confirmation_dt
									 FROM   dbo.TMP_S_CONFIRMATION_METHOD_BASE 
									 GROUP BY  investigation_key,CONFIRMATION_METHOD_DESC,confirmation_dt
								  )MAIN
								 PIVOT
							   (
									MAX(CONFIRMATION_METHOD_DESC) For CONFIRMATION_METHOD_DESC in
				 
									('+
									 @CONFIRMATION_METHOD_DESC
								  +')
								)as P) as c

							'
							PRINT (@sqlQuery)
							EXEC sp_executesql  @sqlQuery

							SELECT @ROWCOUNT_NO = @@ROWCOUNT;

											INSERT INTO [DBO].[JOB_FLOW_LOG] 
											(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
											VALUES(@BATCH_ID,'INV_SUMM_DATAMART','INV_SUMM_DATAMART','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							----added
							BEGIN

								ALTER TABLE dbo.TMP_S_CONFIRMATION_METHOD_PIVOT 
								ADD CONFIRMATION_METHOD varchar(2000)

							END

					  ;WITH  CTE as 
						
						
							(
								select Investigation_key, 
								(    STUFF(
											(SELECT ' | ' + CAST(CMB.[CONFIRMATION_METHOD_DESC] AS varchar(2000))
											 FROM TMP_S_CONFIRMATION_METHOD_Base CMB
											 inner join dbo.TMP_S_CONFIRMATION_METHOD_PIVOT p on CMB.INVESTIGATION_KEY=p.INVESTIGATION_KEY
										     WHERE CMB.[INVESTIGATION_KEY] =CMP.[INVESTIGATION_KEY]
											 FOR XML PATH('')
											)
										   , 2 ,1, ''
										  )

								 ) AS CODE_DESC_TXT_List
								from  dbo.TMP_S_CONFIRMATION_METHOD_Pivot CMP  
							----	where [CONFIRMATION_dt] is not null ----Commended on 3/29/2021
								group by [INVESTIGATION_KEY]
							)

						UPDATE CMP
						set CMP.confirmation_Method = RTRIM(ltrim(cte1.CODE_DESC_TXT_List))
						from  dbo.TMP_S_CONFIRMATION_METHOD_PIVOT CMP, 
									 CTE CTE1
						where CMP.[INVESTIGATION_KEY] = CTE1.[INVESTIGATION_KEY] ;

		    COMMIT TRANSACTION;
	--------------------------------------5. Create Table TMP_S_PATIENT_LOCATION_KEY

	    BEGIN TRANSACTION

					   SET @PROC_STEP_NO = 5;
					   SET @PROC_STEP_NAME = ' GENERATING  TMP_S_PATIENT_LOCATION_KEY'; 
						IF OBJECT_ID('dbo.TMP_S_PATIENT_LOCATION_KEY', 'U') IS NOT NULL   
						drop table dbo.TMP_S_PATIENT_LOCATION_KEY  ;


						SELECT A.*, 
						       B.Confirmation_Method,
						       B.Confirmation_dt as ConfirmationDTE
						      
						INTO dbo.TMP_S_PATIENT_LOCATION_KEY 
						FROM dbo.TMP_S_CONFIRMATION_METHOD_PIVOT B
						INNER  JOIN dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT A ON A.INVESTIGATION_KEY = B.INVESTIGATION_KEY


					SELECT @ROWCOUNT_NO = @@ROWCOUNT;

									INSERT INTO [DBO].[JOB_FLOW_LOG] 
									(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
									VALUES(@BATCH_ID,'INV_SUMM_DATAMART','INV_SUMM_DATAMART','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

		COMMIT TRANSACTION;
---------------------------------------------------6. Create Table  TMP_S_PATIENTS_INFO
	    BEGIN TRANSACTION

					   SET @PROC_STEP_NO = 6;
					   SET @PROC_STEP_NAME = ' GENERATING  TMP_S_PATIENTS_INFO'; 

						IF OBJECT_ID('dbo.TMP_S_PATIENTS_INFO', 'U') IS NOT NULL   
						drop table dbo.TMP_S_PATIENTS_INFO  ;


						SELECT   KEYS.*,
						         C.CONDITION_DESC AS  'DISEASE',
	                             C.CONDITION_CD AS  'DISEASE_CD'
								 INTO dbo.TMP_S_PATIENTS_INFO
									FROM dbo.TMP_S_PATIENT_LOCATION_KEY keys
									INNER JOIN  dbo.CASE_COUNT CC with (nolock) ON keys.investigation_key=CC.investigation_key
									INNER JOIN  dbo.[CONDITION] C with (nolock) ON C.CONDITION_KEY=CC.CONDITION_KEY 
								ORDER BY CONDITION_DESC;

						SELECT @ROWCOUNT_NO = @@ROWCOUNT;

									INSERT INTO [DBO].[JOB_FLOW_LOG] 
									(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
									VALUES(@BATCH_ID,'INV_SUMM_DATAMART','INV_SUMM_DATAMART','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

		COMMIT TRANSACTION;
---------------------------------------------------7. Create Table TMP_S_PHYSICIANS_INFO

	    BEGIN TRANSACTION

					   SET @PROC_STEP_NO = 7;
					   SET @PROC_STEP_NAME = ' GENERATING  TMP_S_PHYSICIANS_INFO'; 

						IF OBJECT_ID('dbo.TMP_S_PHYSICIANS_INFO', 'U') IS NOT NULL   
						drop table dbo.TMP_S_PHYSICIANS_INFO  ;


						 SELECT PI.*,
							PROVIDER_LAST_NAME AS  'PHYSICIAN_LAST_NAME',
							PROVIDER_FIRST_NAME AS  'PHYSICIAN_FIRST_NAME' 
							INTO dbo.TMP_S_PHYSICIANS_INFO
							FROM dbo.TMP_S_PATIENTS_INFO PI  
							LEFT OUTER JOIN [dbo].[D_PROVIDER] PR with (nolock) ON PI.PHYSICIAN_KEY =pr.PROVIDER_KEY
							ORDER BY PATIENT_KEY;

	
						SELECT @ROWCOUNT_NO = @@ROWCOUNT;

									INSERT INTO [DBO].[JOB_FLOW_LOG] 
									(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
									VALUES(@BATCH_ID,'INV_SUMM_DATAMART','INV_SUMM_DATAMART','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

	    COMMIT TRANSACTION		

	------------------------------------------8. Create Table  dbo.TMP_S_PATIENTS_DETAIL -----
	
	    BEGIN TRANSACTION

					   SET @PROC_STEP_NO = 8;
					   SET @PROC_STEP_NAME = ' GENERATING dbo.TMP_S_PATIENTS_DETAIL'; 

						IF OBJECT_ID('dbo.TMP_S_PATIENTS_DETAIL', 'U') IS NOT NULL   
						drop table dbo.TMP_S_PATIENTS_DETAIL  ;

						 SELECT PY.*,
								PA.[PATIENT_FIRST_NAME],
								PA.[PATIENT_LAST_NAME],
								PA.[PATIENT_COUNTY],
								PA.[PATIENT_COUNTY_CODE],
								PA.[PATIENT_STREET_ADDRESS_1],
								PA.[PATIENT_STREET_ADDRESS_2],
								PA.[PATIENT_CITY],
								PA.[PATIENT_STATE],
								PA.[PATIENT_ZIP]	,						
								PA.[PATIENT_ETHNICITY],
								PA.[PATIENT_LOCAL_ID],
								PA.[PATIENT_DOB],
								PA.[PATIENT_CURRENT_SEX],
								PA.[PATIENT_AGE_REPORTED],
								PA.[PATIENT_AGE_REPORTED_UNIT]	,
								PA.[PATIENT_RACE_CALCULATED],
								PA.[PATIENT_RACE_CALC_DETAILS]
								INTO dbo.TMP_S_PATIENTS_DETAIL			
								FROM dbo.TMP_S_PHYSICIANS_INFO PY 
							    LEFT OUTER JOIN  [dbo].[D_PATIENT]PA with (nolock) ON PY.[PATIENT_KEY]=PA.[PATIENT_KEY]
							
								    SELECT @ROWCOUNT_NO = @@ROWCOUNT;

									INSERT INTO [DBO].[JOB_FLOW_LOG] 
									(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
									VALUES(@BATCH_ID,'INV_SUMM_DATAMART','INV_SUMM_DATAMART','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

	    COMMIT TRANSACTION	
	--------------------------------------9. Create Table dbo.TMP_S_INV_WITH_USER
	    BEGIN TRANSACTION

					   SET @PROC_STEP_NO = 9;
					   SET @PROC_STEP_NAME = ' GENERATING TMP_S_INV_WITH_USER'; 

						IF OBJECT_ID('dbo.TMP_S_INV_WITH_USER', 'U') IS NOT NULL   
						drop table dbo.TMP_S_INV_WITH_USER  ;

		
								SELECT A.*,  
									   B.FIRST_NM AS CREATEUSER_FIRST_NM,
								       B.LAST_NM  AS CREATEUSER_LAST_NM,
									   C.FIRST_NM AS EDITUSER_FIRST_NM, 
									   C.LAST_NM  AS EDITUSER_LAST_NM
								INTO dbo.TMP_S_INV_WITH_USER
								FROM dbo.TMP_S_PATIENTS_DETAIL A 
    							LEFT JOIN dbo.[USER_PROFILE]B with (nolock) ON A.INVESTIGATION_CREATED_BY=B.[NEDSS_ENTRY_ID]
								LEFT JOIN dbo.[USER_PROFILE]C with (nolock) ON A.INVESTIGATION_LAST_UPDTD_BY=C.NEDSS_ENTRY_ID;

								--Bigint to Varchar conversion
								Begin		
								ALTER TABLE dbo.TMP_S_INV_WITH_USER ALTER COLUMN [INVESTIGATION_CREATED_BY] varchar(200)
                                ALTER TABLE dbo.TMP_S_INV_WITH_USER ALTER COLUMN [INVESTIGATION_LAST_UPDTD_BY]varchar(200)
								END



										
										          UPDATE dbo.TMP_S_INV_WITH_USER
													 set INVESTIGATION_CREATED_BY = CAST(( Case
						                            when len(rtrim( CREATEUSER_LAST_NM)) > 0 and len(rtrim(CREATEUSER_FIRST_NM))> 0 
													    then rtrim( CREATEUSER_LAST_NM)+','+rtrim(CREATEUSER_FIRST_NM)
													when len(rtrim( CREATEUSER_FIRST_NM)) > 0  
													    then rtrim( CREATEUSER_FIRST_NM)
                                                    when  len(rtrim(CREATEUSER_LAST_NM))> 0 
													    then rtrim(CREATEUSER_LAST_NM)
													else ''
                                                  END
												)as varchar(50));
						

				
											 UPDATE  dbo.TMP_S_INV_WITH_USER
													  set INVESTIGATION_LAST_UPDTD_BY = CAST(( Case
													when len(rtrim( CREATEUSER_LAST_NM)) > 0 and len(rtrim(CREATEUSER_FIRST_NM))> 0 
														then rtrim( CREATEUSER_LAST_NM)+','+rtrim(CREATEUSER_FIRST_NM)
													when len(rtrim( CREATEUSER_FIRST_NM)) > 0  
														then rtrim( CREATEUSER_FIRST_NM)
													when  len(rtrim(CREATEUSER_LAST_NM))> 0 
														then rtrim(CREATEUSER_LAST_NM)
													else ''
												 END
												)as varchar(50));
						
						            SELECT @ROWCOUNT_NO = @@ROWCOUNT;

									INSERT INTO [DBO].[JOB_FLOW_LOG] 
									(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
									VALUES(@BATCH_ID,'INV_SUMM_DATAMART','INV_SUMM_DATAMART','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  
        COMMIT TRANSACTION

------------------------------------------------------10. Create Table  dbo.TMP_S_INV_SUMM_DATAMART_INIT


	    BEGIN TRANSACTION

					   SET @PROC_STEP_NO = 10;
					   SET @PROC_STEP_NAME = ' GENERATING TMP_S_INV_SUMM_DATAMART_INIT'; 

						IF OBJECT_ID('dbo.TMP_S_INV_SUMM_DATAMART_INIT', 'U') IS NOT NULL   
						drop table dbo.TMP_S_INV_SUMM_DATAMART_INIT  ;
 
											--------added on 5-26-2021
	                         ; With CTE as ( 
									        SELECT A.*, 
											NOTI.NOTIFICATION_STATUS,
											NOTI.NOTIFICATION_LOCAL_ID, 
											NOTI.NOTIFICATION_SUBMITTED_BY ,
											RDB_DATE.DATE_MM_DD_YYYY  AS 'NOTIFICATION_CREATE_DATE', 
											RDB_DATE_SENT.DATE_MM_DD_YYYY  AS  'NOTIFICATION_SENT_DATE',
											RDB_DATE_UPD.DATE_MM_DD_YYYY  AS  'NOTIFICATION_LAST_UPDATED_DATE',
											INVESTIGATION_LAST_UPDTD_BY AS  'NOTIFICATION_LAST_UPDATED_USER',
											RTRIM(Ltrim(NOTIFUSER.LAST_NM)) +', '+RTRIM(Ltrim(NOTIFUSER.FIRST_NM)) AS  'NOTIFICATION_SUBMITTER',
											ROW_NUMBER() OVER (Partition by A.Investigation_Key Order by RDB_DATE.DATE_MM_DD_YYYY ASC )as rn
										----	INTO dbo.TMP_S_INV_SUMM_DATAMART_INIT
											FROM dbo.TMP_S_INV_WITH_USER A

											LEFT OUTER JOIN [dbo].[NOTIFICATION_EVENT] NOT_EVENT with (nolock) ON A.INVESTIGATION_KEY=NOT_EVENT.INVESTIGATION_KEY
											LEFT OUTER JOIN [dbo].[NOTIFICATION]            NOTI with (nolock) ON NOTI.NOTIFICATION_KEY=NOT_EVENT.NOTIFICATION_KEY
											LEFT OUTER JOIN [dbo].[RDB_DATE]		    RDB_DATE with (nolock) ON NOT_EVENT.NOTIFICATION_SUBMIT_DT_KEY= RDB_DATE.DATE_KEY
											LEFT OUTER JOIN [dbo].[RDB_DATE]       RDB_DATE_SENT with (nolock) ON NOT_EVENT.NOTIFICATION_SENT_DT_KEY= RDB_DATE_SENT.DATE_KEY
											LEFT OUTER JOIN [dbo].[RDB_DATE]        RDB_DATE_UPD with (nolock) ON NOT_EVENT.[NOTIFICATION_KEY]= RDB_DATE_UPD.DATE_KEY   
											LEFT OUTER JOIN [dbo].[USER_PROFILE]       NOTIFUSER with (nolock) ON NOTI.NOTIFICATION_SUBMITTED_BY=NOTIFUSER.NEDSS_ENTRY_ID
										
										)
									Select *  INTO dbo.TMP_S_INV_SUMM_DATAMART_INIT from CTE where rn=1
									
									SELECT @ROWCOUNT_NO = @@ROWCOUNT;

									INSERT INTO [DBO].[JOB_FLOW_LOG] 
									(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
									VALUES(@BATCH_ID,'INV_SUMM_DATAMART','INV_SUMM_DATAMART','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  
       COMMIT TRANSACTION
---------------------------------------------------------11. Create table TMP_InvLab
       BEGIN TRANSACTION

					   SET @PROC_STEP_NO = 11;
					   SET @PROC_STEP_NAME = ' GENERATING TMP_InvLab'; 


							IF OBJECT_ID('dbo.TMP_InvLab', 'U') IS NOT NULL   
							drop table dbo.TMP_InvLab ;
											
									    SELECT L.INVESTIGATION_KEY, L.LAB_TEST_KEY
												INTO  dbo.TMP_InvLab
												FROM  dbo.LAB_TEST_RESULT L   with (nolock)
												INNER JOIN dbo.INVESTIGATION I with (nolock)ON L.INVESTIGATION_KEY = I.INVESTIGATION_KEY
										 WHERE (L.LAB_TEST_KEY IN(SELECT  LAB_TEST_KEY FROM dbo.LAB_TEST)) 
												  AND (L.INVESTIGATION_KEY <> 1) 	AND (I.RECORD_STATUS_CD = 'ACTIVE')
										 ORDER BY LAB_TEST_KEY;
										

							SELECT @ROWCOUNT_NO = @@ROWCOUNT;

									INSERT INTO [DBO].[JOB_FLOW_LOG] 
									(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
									VALUES(@BATCH_ID,'INV_SUMM_DATAMART','D_INV_Summ_DataMart','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  
        COMMIT TRANSACTION
------------------------------------------------------------12 Create Table  TMP_Lab------------------------------------------------------
        BEGIN TRANSACTION

					   SET @PROC_STEP_NO = 12;
					   SET @PROC_STEP_NAME = 'GENERATING TMP_Lab'; 


							IF OBJECT_ID('dbo.TMP_Lab', 'U') IS NOT NULL   
							drop table dbo.TMP_Lab ;

												SELECT 
													 lab_test_key,
													 lab_rpt_local_id 
												INTO dbo.TMP_Lab
												FROM  dbo.Lab_test with (nolock) order by lab_test_key;

								SELECT @ROWCOUNT_NO = @@ROWCOUNT;

									INSERT INTO [DBO].[JOB_FLOW_LOG] 
									(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
									VALUES(@BATCH_ID,'INV_SUMM_DATAMART','D_INV_Summ_DataMart','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  
         COMMIT TRANSACTION

--------------------------------------------------13. Create Table  Tmp_BothTable--------------------------------------

         BEGIN TRANSACTION

					   SET @PROC_STEP_NO = 13;
					   SET @PROC_STEP_NAME = 'GENERATING  Tmp_BothTable'; 


							IF OBJECT_ID('dbo.TMP_BothTable', 'U') IS NOT NULL   
							drop table  dbo.TMP_BothTable ;



							          SELECT INV.INVESTIGATION_KEY,
							           INV.LAB_TEST_KEY as INVTestKey,
									   L.LAB_TEST_KEY   as LabTestKey,
									   L.LAB_RPT_LOCAL_ID
									  INTO  dbo.TMP_BothTable
									  FROM dbo.TMP_InvLab INV,
									       dbo.TMP_Lab  L
										   WHERE INV.LAB_TEST_KEY = L.LAB_TEST_KEY

                     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

									INSERT INTO [DBO].[JOB_FLOW_LOG] 
									(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
									VALUES(@BATCH_ID,'INV_SUMM_DATAMART','INV_SUMM_DATAMART','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  
        COMMIT TRANSACTION
-----------------------------------------------------------------14. Create Table -TMP_Inv2Labs----------------------------------------
        BEGIN TRANSACTION
													 SET @PROC_STEP_NO = 14;
													 SET @PROC_STEP_NAME = ' GENERATING  TMP_Inv2Labs'; 


													IF OBJECT_ID('dbo.TMP_Inv2Labs', 'U') IS NOT NULL   
											         drop table dbo.TMP_Inv2Labs ;
																		
															SELECT distinct 
															 b.investigation_key, 
											                 b.LabTestKey as lab_test_key,
                                                             l.lab_rpt_LOCAL_ID, 
															 l.LAB_RPT_RECEIVED_BY_PH_DT, 
															 l.SPECIMEN_COLLECTION_DT,
                                                             l.RESULTED_LAB_TEST_CD_DESC, 
															 l.RESULTEDTEST_VAL_CD_DESC,
															 l.NUMERIC_RESULT_WITHUNITS,             
															 l.LAB_RESULT_TXT_VAL, 
															 l.LAB_RESULT_COMMENTS,
                                                             l.ELR_IND
															  INTO dbo.TMP_Inv2Labs
                                                              FROM dbo.TMP_BothTable b 
															INNER JOIN  dbo.lab100   l with (nolock) on  l.LAB_RPT_LOCAL_ID = b.LAB_RPT_LOCAL_ID
																order by b.investigation_key;

                                     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

									INSERT INTO [DBO].[JOB_FLOW_LOG] 
									(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
									VALUES(@BATCH_ID,'INV_SUMM_DATAMART','INV_SUMM_DATAMART','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  
        COMMIT TRANSACTION
------------------------------------------------------------------15. Create Table TMP_SPECIMEN_COLLECTION---------------------------------------------------------
        BEGIN TRANSACTION
													 SET @PROC_STEP_NO = 15;
													 SET @PROC_STEP_NAME = ' GENERATING  TMP_SPECIMEN_COLLECTION'; 


													IF OBJECT_ID('dbo.TMP_SPECIMEN_COLLECTION', 'U') IS NOT NULL   
											         drop table dbo.TMP_SPECIMEN_COLLECTION ;
									   									
														  SELECT DISTINCT  INVESTIGATION_KEY ,
														  MIN(SPECIMEN_COLLECTION_DT) as  EARLIEST_SPECIMEN_COLLECTION_DT
														  INTO  dbo.TMP_SPECIMEN_COLLECTION
														  from  dbo.TMP_Inv2Labs with (nolock)where SPECIMEN_COLLECTION_DT is not null 
														  Group by INVESTIGATION_KEY 
														  order by INVESTIGATION_KEY

											SELECT @ROWCOUNT_NO = @@ROWCOUNT;

											INSERT INTO [DBO].[JOB_FLOW_LOG] 
											(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
											VALUES(@BATCH_ID,'INV_SUMM_DATAMART','INV_SUMM_DATAMART','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  
         COMMIT TRANSACTION
-----------------------------------------------------------------16. Create Table TMP_CASE_LAB_DATAMART_MODIFIED---------------------------------
         BEGIN TRANSACTION
													 SET @PROC_STEP_NO = 16;
													 SET @PROC_STEP_NAME = 'GENERATING  TMP_CASE_LAB_DATAMART_MODIFIED'; 


													IF OBJECT_ID('dbo.TMP_CASE_LAB_DATAMART_MODIFIED', 'U') IS NOT NULL   
											         drop table  dbo.TMP_CASE_LAB_DATAMART_MODIFIED ;

									 								
																		
														 SELECT DISTINCT C.INVESTIGATION_KEY ,
														  SC.EARLIEST_SPECIMEN_COLLECTION_DT 
														  INTO  dbo.TMP_CASE_LAB_DATAMART_MODIFIED 
														  from  dbo.[CASE_LAB_DATAMART] C with (nolock) 
														  Left OUTER JOIN dbo.TMP_SPECIMEN_COLLECTION SC with (nolock) ON SC.INVESTIGATION_KEY = C.INVESTIGATION_KEY
														  order by C.INVESTIGATION_KEY asc;

											SELECT @ROWCOUNT_NO = @@ROWCOUNT;

											INSERT INTO [DBO].[JOB_FLOW_LOG] 
											(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
											VALUES(@BATCH_ID,'INV_SUMM_DATAMART','INV_SUMM_DATAMART','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  
          COMMIT TRANSACTION
------------------------------------------------------17. Create Table  TMP_INV_SUMM_DATAMART

          BEGIN TRANSACTION

					           SET @PROC_STEP_NO = 17;
					           SET @PROC_STEP_NAME = 'GENERATING TMP_INV_SUMM_DATAMART'; 

				              IF OBJECT_ID('dbo.TMP_INV_SUMM_DATAMART', 'U') IS NOT NULL   
						      drop table    dbo.TMP_INV_SUMM_DATAMART ;
				
				
								
								SELECT DISTINCT 
										 A.INVESTIGATION_KEY                 ---1
										,A.PATIENT_LOCAL_ID                  ---2
										,A.PATIENT_KEY	                    ---3
										,A.INVESTIGATION_LOCAL_ID            ----4
										,substring(A.DISEASE,1,50) as DISEASE ----5---------------------------------------5-24-2021   Changed
										,A.DISEASE_CD                        ----6
										,A.PATIENT_FIRST_NAME               ----7	
										,A.PATIENT_LAST_NAME                     ----8
										,CAST(A.PATIENT_DOB as DATE) as PATIENT_DOB   ----9
										,A.PATIENT_CURRENT_SEX             ----10	
										,A.PATIENT_AGE_REPORTED as AGE_REPORTED	----11
										,rtrim(ltrim(A.PATIENT_AGE_REPORTED_UNIT)) as AGE_REPORTED_UNIT-----12
										,rtrim(ltrim(A.PATIENT_STREET_ADDRESS_1)) as PATIENT_STREET_ADDRESS_1    ----13	
										,rtrim(ltrim(A.PATIENT_STREET_ADDRESS_2)) as PATIENT_STREET_ADDRESS_2     ----14	
										,rtrim(ltrim(A.PATIENT_CITY)) as PATIENT_CITY        	-----15
										,A.PATIENT_STATE	------16
										,A.PATIENT_ZIP     ------17
										,A.PATIENT_COUNTY	----18
										,rtrim(ltrim(A.PATIENT_ETHNICITY))  as PATIENT_ETHNICITY       ---19
										,A.PATIENT_RACE_CALCULATED	 as RACE_CALCULATED----20
										,A.PATIENT_RACE_CALC_DETAILS as RACE_CALC_DETAILS     ----21
										,Substring(A.INVESTIGATION_STATUS,1,50) as	INVESTIGATION_STATUS  ----22 -----------------6/2/2021
										,A.EARLIEST_RPT_TO_CNTY_DT	----23
										,A.EARLIEST_RPT_TO_STATE_DT   ----24	
										,A.DIAGNOSIS_DATE ------25
										,A.ILLNESS_ONSET_DATE      -----26	
										,Substring(A.CASE_STATUS,1,50) as CASE_STATUS  ----27	---------------6/2/2021	
										,A.MMWR_WEEK   -----28	
										,A.MMWR_YEAR           ----29
										----,CAST (A.INVESTIGATION_CREATE_DATE as DATE) as INVESTIGATION_CREATE_DATE  ----30	
										,A.INVESTIGATION_CREATE_DATE 
										,A.INVESTIGATION_CREATED_BY     ---31	
										,A.INVESTIGATION_LAST_UPDTD_DATE ----32
										,A.NOTIFICATION_STATUS          ----33
										,substring(A.INVESTIGATION_LAST_UPDTD_BY,1,50) as INVESTIGATION_LAST_UPDTD_BY    ----34-----------6/2/2021-
										,A.PROGRAM_JURISDICTION_OID      ----35
										,A.PROGRAM_AREA          ----36
										,A.PHYSICIAN_LAST_NAME	----37
										,A.PHYSICIAN_FIRST_NAME     ----38
										,A.NOTIFICATION_LOCAL_ID       ---39
										,A.NOTIFICATION_CREATE_DATE   ---40	
									---	,CAST(A.NOTIFICATION_SENT_DATE as DATE) as NOTIFICATION_SENT_DATE       ---41-----not needed since there is no field
										,Substring(A.NOTIFICATION_SUBMITTER,1,50) as NOTIFICATION_SUBMITTER      ---41-------6/2/2021 ---
										,A.NOTIFICATION_LAST_UPDATED_DATE	---42
										,Substring(A.NOTIFICATION_LAST_UPDATED_USER,1,50) as NOTIFICATION_LAST_UPDATED_USER	------43-----------6/2/2021
										,A.FIRST_POSITIVE_CULTURE_DT      -----44
										,A.INV_START_DT -----45
										,A.HSPTL_ADMISSION_DT   ---46
										,A.INV_RPT_DT  ---47	

										,A.ConfirmationDTE  as CONFIRMATION_DT   ----48 
										,A.CONFIRMATION_METHOD        ---49
										,Substring(A.CURR_PROCESS_STATE,1,50) as CURR_PROCESS_STATE     ----50---------------6/2/2021
										,Substring(A.JURISDICTION_NM,1,100) as JURISDICTION_NM      	----51---------------6/2/2021
									
										,A.PATIENT_COUNTY_CODE    ---52

										,Substring(B.LABORATORY_INFORMATION,1,4000) as LABORATORY_INFORMATION ----53----6/2/2021
										,CAST(S.EARLIEST_SPECIMEN_COLLECTION_DT as DATE) AS EARLIEST_SPECIMEN_COLLECT_DATE ----54 ----dont have this field in the table
										,CAST(NULL as datetime ) as EVENT_DATE
									    ,CAST(NULL as  varchar(200))as EVENT_DATE_TYPE
										
										 INTO dbo.TMP_INV_SUMM_DATAMART
										 FROM dbo.TMP_S_INV_SUMM_DATAMART_INIT A 
									    LEFT OUTER JOIN [dbo].[CASE_LAB_DATAMART]  B  with (nolock) ON 	A.INVESTIGATION_KEY=B.INVESTIGATION_KEY
										LEFT OUTER JOIN dbo.TMP_CASE_LAB_DATAMART_MODIFIED S with (nolock) ON A.INVESTIGATION_KEY=S.INVESTIGATION_KEY 
										

										SELECT @ROWCOUNT_NO = @@ROWCOUNT;

									INSERT INTO [DBO].[JOB_FLOW_LOG] 
									(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
									VALUES(@BATCH_ID,'INV_SUMM_DATAMART','INV_SUMM_DATAMART','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);

										

											/* sas code
											IF MISSING(NOTIFICATION_CREATE_DATE) THEN DO;			 					
													NOTIFICATION_LAST_UPDATED_DATE=.;
												 END;
												IF MISSING(NOTIFICATION_CREATE_DATE) THEN DO;
													NOTIFICATION_LAST_UPDATED_USER=.;
												 END;
											---No need---2/15/2021 */
											 UPDATE  dbo.TMP_INV_SUMM_DATAMART
											SET NOTIFICATION_LAST_UPDATED_DATE =Case when NOTIFICATION_CREATE_DATE is null then null  else NOTIFICATION_CREATE_DATE end

											UPDATE  dbo.TMP_INV_SUMM_DATAMART
								            SET NOTIFICATION_LAST_UPDATED_USER =Case when NOTIFICATION_CREATE_DATE is null then null else NOTIFICATION_LAST_UPDATED_USER end
											
											

												/* THIS IS THE DEFAULT VALUE --sas code
													EVENT_DATE =INVESTIGATION_CREATE_DATE; 
													EVENT_DATE_TYPE	='Investigation Add Date';*/

											UPDATE dbo.TMP_INV_SUMM_DATAMART
											SET EVENT_DATE = INVESTIGATION_CREATE_DATE,
											EVENT_DATE_TYPE	='Investigation Add Date'

											UPDATE  dbo.TMP_INV_SUMM_DATAMART
											SET EVENT_DATE = HSPTL_ADMISSION_DT,
												EVENT_DATE_TYPE	='Hospitalization Admit Date'
												WHERE 	EVENT_DATE > HSPTL_ADMISSION_DT AND HSPTL_ADMISSION_DT is NOT NULL

											UPDATE  dbo.TMP_INV_SUMM_DATAMART
											SET EVENT_DATE = CONFIRMATION_DT,
												EVENT_DATE_TYPE	='Confirmation Date'
												WHERE 	EVENT_DATE >CONFIRMATION_DT AND CONFIRMATION_DT is Not NULL

											UPDATE  dbo.TMP_INV_SUMM_DATAMART
											SET EVENT_DATE = INV_START_DT,
												EVENT_DATE_TYPE	='Investigation Start Date'
												WHERE 	EVENT_DATE > INV_START_DT AND INV_START_DT is NOT NULL

											UPDATE  dbo.TMP_INV_SUMM_DATAMART
											SET EVENT_DATE = INV_RPT_DT,
												EVENT_DATE_TYPE	='Date of Report'
												WHERE 	EVENT_DATE > INV_RPT_DT AND INV_RPT_DT is NOT NULL

											UPDATE  dbo.TMP_INV_SUMM_DATAMART
											SET EVENT_DATE = EARLIEST_RPT_TO_STATE_DT,
												EVENT_DATE_TYPE	='Earliest date received by the state health department'
												WHERE 	EVENT_DATE > EARLIEST_RPT_TO_STATE_DT AND EARLIEST_RPT_TO_STATE_DT is NOT NULL
											
											UPDATE  dbo.TMP_INV_SUMM_DATAMART
											SET EVENT_DATE =  EARLIEST_RPT_TO_CNTY_DT,
												EVENT_DATE_TYPE	='Earliest date received by the county/local health department'
												WHERE 	EVENT_DATE >  EARLIEST_RPT_TO_CNTY_DT AND EARLIEST_RPT_TO_CNTY_DT is NOT NULL

											UPDATE  dbo.TMP_INV_SUMM_DATAMART
											SET EVENT_DATE =   DIAGNOSIS_DATE,
												EVENT_DATE_TYPE	='Date of Diagnosis'
												WHERE 	 DIAGNOSIS_DATE is NOT NULL
										

											UPDATE  dbo.TMP_INV_SUMM_DATAMART
											SET EVENT_DATE =  EARLIEST_SPECIMEN_COLLECT_DATE,
												EVENT_DATE_TYPE	='Specimen Collection Date of Earliest Associated Lab'
												WHERE 	  EARLIEST_SPECIMEN_COLLECT_DATE is not NULL
										
											UPDATE  dbo.TMP_INV_SUMM_DATAMART
											SET EVENT_DATE = ILLNESS_ONSET_DATE,
												EVENT_DATE_TYPE	='Illness Onset Date'
												WHERE 	 ILLNESS_ONSET_DATE IS NOT NULL 

											

									
								
        COMMIT TRANSACTION
----------------------------------------18.  UPDATE dbo.INV_SUMM_DATAMART--------------------------------------------------------
        BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 18;
			SET @PROC_STEP_NAME = 'dbo.INV_SUMM_DATAMART'; 


							UPDATE  dbo.INV_SUMM_DATAMART
  							 SET  [INVESTIGATION_KEY]             =  ISD.[INVESTIGATION_KEY],---1
							      [PATIENT_LOCAL_ID]              =  ISD.[PATIENT_LOCAL_ID] ,---2
							      [PATIENT_KEY]                   =  ISD.[PATIENT_KEY],----3
	                              [INVESTIGATION_LOCAL_ID]        =  ISD.[INVESTIGATION_LOCAL_ID],---4
								  [DISEASE]                       =  ISD.[DISEASE] ,---5
								  [DISEASE_CD]                    =  ISD.[DISEASE_CD] ,---6
	                              [PATIENT_FIRST_NAME]            =  ISD.[PATIENT_FIRST_NAME] ,---7
								  [PATIENT_LAST_NAME]             =  ISD.[PATIENT_LAST_NAME],---8
								  [PATIENT_DOB]                   =  ISD.[PATIENT_DOB]  ,---9
	                              [PATIENT_CURRENT_SEX]           =  ISD.[PATIENT_CURRENT_SEX] ,---10
								  [AGE_REPORTED]                  =  ISD.[AGE_REPORTED],----11
								  [AGE_REPORTED_UNIT]             =  ISD.[AGE_REPORTED_UNIT],----12
	                              [PATIENT_STREET_ADDRESS_1]      =  ISD.[PATIENT_STREET_ADDRESS_1] ,----13
								  [PATIENT_STREET_ADDRESS_2]      =  ISD.[PATIENT_STREET_ADDRESS_2] ,-----14
								  [PATIENT_CITY]                  =  ISD.[PATIENT_CITY],----15
	                              [PATIENT_STATE]                 =  ISD.[PATIENT_STATE] ,----16
								  [PATIENT_ZIP]                   =  ISD.[PATIENT_ZIP] ,----17
								  [PATIENT_COUNTY]                =  ISD.[PATIENT_COUNTY] ,---18
								  [PATIENT_ETHNICITY]             =  ISD.[PATIENT_ETHNICITY] ,----19
	                              [RACE_CALCULATED]               =  ISD.[RACE_CALCULATED],---table name changed---20
								  [RACE_CALC_DETAILS]             =  ISD.[RACE_CALC_DETAILS],---table name changed---21
								  [INVESTIGATION_STATUS]          =  ISD.[INVESTIGATION_STATUS],---22
	                              [EARLIEST_RPT_TO_CNTY_DT]       =  ISD.[EARLIEST_RPT_TO_CNTY_DT],---23
								  [EARLIEST_RPT_TO_STATE_DT]      =  ISD.[EARLIEST_RPT_TO_STATE_DT]  ,---24
								  [DIAGNOSIS_DATE]                =  ISD.[DIAGNOSIS_DATE] ,---25
	                              [ILLNESS_ONSET_DATE]            =  ISD.[ILLNESS_ONSET_DATE],---26
								  [CASE_STATUS]                   =  ISD.[CASE_STATUS],---27
								  [MMWR_WEEK]                     =  ISD.[MMWR_WEEK] ,---28
								  [MMWR_YEAR]                     =  ISD.[MMWR_YEAR] ,---29
								  [INVESTIGATION_CREATE_DATE]     =  ISD.[INVESTIGATION_CREATE_DATE] ,---30
	                              [INVESTIGATION_CREATED_BY]      =  ISD.[INVESTIGATION_CREATED_BY],---31
								  [INVESTIGATION_LAST_UPDTD_DATE] =  ISD.[INVESTIGATION_LAST_UPDTD_DATE],---32
								  [NOTIFICATION_STATUS]            = ISD.[NOTIFICATION_STATUS]  ,----33
								  [INVESTIGATION_LAST_UPDTD_BY]    = ISD.[INVESTIGATION_LAST_UPDTD_BY] ,----34
								  [PROGRAM_JURISDICTION_OID]       = ISD.[PROGRAM_JURISDICTION_OID] ,---35
								  [PROGRAM_AREA]                   = ISD.[PROGRAM_AREA],-----36
								  [PHYSICIAN_LAST_NAME]            = ISD.[PHYSICIAN_LAST_NAME] ,----37
								  [PHYSICIAN_FIRST_NAME]           = ISD.[PHYSICIAN_FIRST_NAME],----38
								  [NOTIFICATION_LOCAL_ID]          = ISD.[NOTIFICATION_LOCAL_ID],----39
								  [NOTIFICATION_CREATE_DATE]       = ISD.[NOTIFICATION_CREATE_DATE] ,----40
						    	--  [NOTIFICATION_SENT_DATE]       = ISD.[NOTIFICATION_SENT_DATE],           ---not there
								  [NOTIFICATION_SUBMITTER]         = ISD.[NOTIFICATION_SUBMITTER]  ,---41
								  [NOTIFICATION_LAST_UPDATED_DATE] = ISD.[NOTIFICATION_LAST_UPDATED_DATE] ,---42
								  [NOTIFICATION_LAST_UPDATED_USER] = ISD.[NOTIFICATION_LAST_UPDATED_USER],---43
								  [FIRST_POSITIVE_CULTURE_DT]      = ISD.[FIRST_POSITIVE_CULTURE_DT] ,---44
								  [INV_START_DT]                   = ISD.[INV_START_DT]  ,---45
								  [HSPTL_ADMISSION_DT]             = ISD.[HSPTL_ADMISSION_DT],---46
								  [INV_RPT_DT]                     = ISD.[INV_RPT_DT],----47
								  [CONFIRMATION_DT]                = ISD.[CONFIRMATION_DT] ,----48
								  [CONFIRMATION_METHOD]            = ISD.[CONFIRMATION_METHOD],----49
								  [CURR_PROCESS_STATE]             = ISD.[CURR_PROCESS_STATE] ,----50
								  [JURISDICTION_NM]                = ISD.[JURISDICTION_NM],---51
								  [PATIENT_COUNTY_CODE]            = ISD.[PATIENT_COUNTY_CODE],---52
								  [LABORATORY_INFORMATION]         = ISD.[LABORATORY_INFORMATION],----53
								  [EARLIEST_SPECIMEN_COLLECT_DATE] = ISD.[EARLIEST_SPECIMEN_COLLECT_DATE],---table changed----54
								  [EVENT_DATE]                     = ISD.[EVENT_DATE] ,---55
								  [EVENT_DATE_TYPE]                 =ISD.[EVENT_DATE_TYPE]----56
								  FROM   dbo.TMP_INV_SUMM_DATAMART ISD
				                  Where ISD.[INVESTIGATION_KEY]  = [dbo].[INV_SUMM_DATAMART].INVESTIGATION_KEY
				                  
								  ---added 2/2/2021
								  /*
								   BEGIN
									 UPDATE T
									 SET T.INVESTIGATION_KEY  = R.rowNum
									 FROM  dbo.TMP_INV_SUMM_DATAMART T
									 JOIN (Select T2.Investigation_Key,ROW_NUMBER() over (Order by T2.Investigation_Key ASC) rowNum From dbo.TMP_INV_SUMM_DATAMART T2
									 ) R on T.INVESTIGATION_KEY = R.INVESTIGATION_KEY
								 END
								  */
					            SELECT @ROWCOUNT_NO = @@ROWCOUNT;

						          INSERT INTO [DBO].[JOB_FLOW_LOG] 
									(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
									VALUES(@BATCH_ID,'INV_SUMM_DATAMART','INV_SUMM_DATAMART','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);

									----Added since lab info was not updating----on 6/8/2021 
										 UPDATE dbo.INV_SUMM_DATAMART
										    SET  [LABORATORY_INFORMATION]         = CLD.[LABORATORY_INFORMATION]
										 FROM    [dbo].[CASE_LAB_DATAMART] as CLD
										    INNER JOIN  dbo.INVESTIGATION  AS I
										       ON CLD.INVESTIGATION_KEY=I.INVESTIGATION_KEY and CLD.INVESTIGATION_LOCAL_ID=I.INV_LOCAL_ID 
										 WHERE CLD.INVESTIGATION_KEY     =INV_SUMM_DATAMART.INVESTIGATION_KEY
										       and   CLD.INVESTIGATION_LOCAL_ID =INV_SUMM_DATAMART.INVESTIGATION_LOCAL_ID
										       and   CLD.PHC_LAST_CHG_TIME = I.LAST_CHG_TIME

									
									 ----Added since lab Specimen info was not updating----on 6/9/2021 
																			
																					
										;With CTE as (
												Select C.INVESTIGATION_KEY,C.INVESTIGATION_LOCAL_ID,LH.LAB_RPT_LAST_UPDATE_DT,I.Last_CHG_TIME,LH.SPECIMEN_COLLECTION_DT ,LH.Event_Date,
												ROW_NUMBER() OVER (PARTITION BY C.INVESTIGATION_KEY 
																						ORDER BY LH.SPECIMEN_COLLECTION_DT ASC
         																		) AS [ROWNO]    
												from 
												[dbo].[CASE_LAB_DATAMART] C 
												INNER JOIN INVESTIGATION   I ON I.INV_LOCAL_ID = C.INVESTIGATION_LOCAL_ID and C.INVESTIGATION_KEY= I.INVESTIGATION_KEY
												INNER JOIN [dbo].[LAB_TEST_RESULT] LTR ON I.INVESTIGATION_KEY =LTR.INVESTIGATION_KEY
												INNER JOIN dbo.Lab_test LT  ON LTR.LAB_TEST_UID=LT.LAB_RPT_UID and LTR.LAB_TEST_KEY=LT.LAB_TEST_KEY
												INNER JOIN  dbo.lab100 LH On LH.LAB_RPT_LOCAL_ID=LT.LAB_RPT_LOCAL_ID and C.PATIENT_LOCAL_ID= LH.PERSON_LOCAL_ID
												WHERE 
	
												LH.LAB_RPT_LAST_UPDATE_DT >I.Last_CHG_TIME  and LH.SPECIMEN_COLLECTION_DT is not null
										          )
											      Select *  INTO #Tmp FROM CTE  where ROWNO=1
												  

											UPDATE   dbo.INV_SUMM_DATAMART
											SET  EARLIEST_SPECIMEN_COLLECT_DATE = t.SPECIMEN_COLLECTION_DT,
											                   [EVENT_DATE]       = t.EVENT_DATE
											FROM #tmp t
											WHERE
												   t.INVESTIGATION_KEY      =INV_SUMM_DATAMART.INVESTIGATION_KEY and 
												   t.INVESTIGATION_LOCAL_ID =INV_SUMM_DATAMART.INVESTIGATION_LOCAL_ID
													
											Drop table  #tmp

											--------------------------- ----Added since lab Specimen info was not updating----on 6/9/2021 

											UPDATE  dbo.INV_SUMM_DATAMART
											SET  EVENT_DATE_TYPE	  ='Specimen Collection Date of Earliest Associated Lab'
											Where EARLIEST_SPECIMEN_COLLECT_DATE is not null
											and LABORATORY_INFORMATION is Not null
											
											------added 6/15/2021
											UPDATE  dbo.INV_SUMM_DATAMART
											SET   EARLIEST_SPECIMEN_COLLECT_DATE=NULL
											WHERE LABORATORY_INFORMATION IS NULL


											UPDATE dbo.INV_SUMM_DATAMART
											SET EVENT_DATE = INVESTIGATION_CREATE_DATE,
											EVENT_DATE_TYPE	='Investigation Add Date'

											UPDATE   dbo.INV_SUMM_DATAMART
											SET EVENT_DATE = HSPTL_ADMISSION_DT,
												EVENT_DATE_TYPE	='Hospitalization Admit Date'
												WHERE 	EVENT_DATE > HSPTL_ADMISSION_DT AND HSPTL_ADMISSION_DT is NOT NULL

											UPDATE   dbo.INV_SUMM_DATAMART
											SET EVENT_DATE = CONFIRMATION_DT,
												EVENT_DATE_TYPE	='Confirmation Date'
												WHERE 	EVENT_DATE >CONFIRMATION_DT AND CONFIRMATION_DT is Not NULL

											UPDATE   dbo.INV_SUMM_DATAMART
											SET EVENT_DATE = INV_START_DT,
												EVENT_DATE_TYPE	='Investigation Start Date'
												WHERE 	EVENT_DATE > INV_START_DT AND INV_START_DT is NOT NULL

											UPDATE   dbo.INV_SUMM_DATAMART
											SET EVENT_DATE = INV_RPT_DT,
												EVENT_DATE_TYPE	='Date of Report'
												WHERE 	EVENT_DATE > INV_RPT_DT AND INV_RPT_DT is NOT NULL

											UPDATE   dbo.INV_SUMM_DATAMART
											SET EVENT_DATE = EARLIEST_RPT_TO_STATE_DT,
												EVENT_DATE_TYPE	='Earliest date received by the state health department'
												WHERE 	EVENT_DATE > EARLIEST_RPT_TO_STATE_DT AND EARLIEST_RPT_TO_STATE_DT is NOT NULL
											
											UPDATE   dbo.INV_SUMM_DATAMART
											SET EVENT_DATE =  EARLIEST_RPT_TO_CNTY_DT,
												EVENT_DATE_TYPE	='Earliest date received by the county/local health department'
												WHERE 	EVENT_DATE >  EARLIEST_RPT_TO_CNTY_DT AND EARLIEST_RPT_TO_CNTY_DT is NOT NULL

											UPDATE  dbo.INV_SUMM_DATAMART
											SET EVENT_DATE =   DIAGNOSIS_DATE,
												EVENT_DATE_TYPE	='Date of Diagnosis'
												WHERE 	 DIAGNOSIS_DATE is NOT NULL
										

											UPDATE   dbo.INV_SUMM_DATAMART
											SET EVENT_DATE =  EARLIEST_SPECIMEN_COLLECT_DATE,
												EVENT_DATE_TYPE	='Specimen Collection Date of Earliest Associated Lab'
												WHERE 	  EARLIEST_SPECIMEN_COLLECT_DATE is not NULL
										
											UPDATE   dbo.INV_SUMM_DATAMART
											SET EVENT_DATE = ILLNESS_ONSET_DATE,
												EVENT_DATE_TYPE	='Illness Onset Date'
												WHERE 	 ILLNESS_ONSET_DATE IS NOT NULL 


											
											
											----adde 3/26/21
											BEGIN
										      Delete inv from  dbo.INV_SUMM_DATAMART INV
										      Inner Join [dbo].[INVESTIGATION] I ON   I.[INVESTIGATION_KEY]=INV.INVESTIGATION_KEY
											  WHERE I.CASE_TYPE= 'I' AND I.RECORD_STATUS_CD = 'INACTIVE'
											END


			COMMIT TRANSACTION;
---------------------------  ----------19. Insert into Final Table

           BEGIN TRANSACTION

					           SET @PROC_STEP_NO = 19;
					           SET @PROC_STEP_NAME = 'INSERTING into INV_SUMM_DATAMART'; 

							    INSERT INTO dbo.INV_SUMM_DATAMART 
							   ( 
							      [INVESTIGATION_KEY],[PATIENT_LOCAL_ID] ,[PATIENT_KEY],
	                              [INVESTIGATION_LOCAL_ID],[DISEASE] ,[DISEASE_CD] ,
	                              [PATIENT_FIRST_NAME] ,[PATIENT_LAST_NAME],[PATIENT_DOB] ,
	                              [PATIENT_CURRENT_SEX] ,[AGE_REPORTED],[AGE_REPORTED_UNIT] ,
	                              [PATIENT_STREET_ADDRESS_1] ,[PATIENT_STREET_ADDRESS_2] ,[PATIENT_CITY] ,
	                              [PATIENT_STATE] ,[PATIENT_ZIP] ,[PATIENT_COUNTY] ,[PATIENT_ETHNICITY] ,
	                              [RACE_CALCULATED],[RACE_CALC_DETAILS] ,[INVESTIGATION_STATUS] ,
	                              [EARLIEST_RPT_TO_CNTY_DT] ,[EARLIEST_RPT_TO_STATE_DT] ,[DIAGNOSIS_DATE] ,
	                              [ILLNESS_ONSET_DATE],[CASE_STATUS],[MMWR_WEEK] ,[MMWR_YEAR] ,[INVESTIGATION_CREATE_DATE] ,
	                              [INVESTIGATION_CREATED_BY],[INVESTIGATION_LAST_UPDTD_DATE],[NOTIFICATION_STATUS] ,
								  [INVESTIGATION_LAST_UPDTD_BY] ,[PROGRAM_JURISDICTION_OID] ,[PROGRAM_AREA],
								  [PHYSICIAN_LAST_NAME] ,[PHYSICIAN_FIRST_NAME],[NOTIFICATION_LOCAL_ID],
								  [NOTIFICATION_CREATE_DATE] ,
								----  [NOTIFICATION_SENT_DATE],
								  [NOTIFICATION_SUBMITTER] ,
								  [NOTIFICATION_LAST_UPDATED_DATE] ,[NOTIFICATION_LAST_UPDATED_USER] ,
								  [FIRST_POSITIVE_CULTURE_DT] ,[INV_START_DT] ,[HSPTL_ADMISSION_DT] ,[INV_RPT_DT] ,
								  [CONFIRMATION_DT] ,[CONFIRMATION_METHOD] ,[CURR_PROCESS_STATE] ,[JURISDICTION_NM],
								  [PATIENT_COUNTY_CODE],[LABORATORY_INFORMATION],[EARLIEST_SPECIMEN_COLLECT_DATE] ,
								  [EVENT_DATE] ,[EVENT_DATE_TYPE]
								   )
									 
									  
															  
								 SELECT 
								        T.INVESTIGATION_KEY , T.PATIENT_LOCAL_ID , T.PATIENT_KEY,	                   
										T.INVESTIGATION_LOCAL_ID ,substring(T.DISEASE ,1,50), T.DISEASE_CD ,                      
										T.PATIENT_FIRST_NAME , T.PATIENT_LAST_NAME, T.PATIENT_DOB  ,
										T.PATIENT_CURRENT_SEX, T.AGE_REPORTED,	T.AGE_REPORTED_UNIT,
										T.PATIENT_STREET_ADDRESS_1 , T.PATIENT_STREET_ADDRESS_2 ,  T.PATIENT_CITY ,        	
										T.PATIENT_STATE,	T.PATIENT_ZIP ,T.PATIENT_COUNTY,T.PATIENT_ETHNICITY ,       
										T.RACE_CALCULATED,T.RACE_CALC_DETAILS ,T.INVESTIGATION_STATUS,	       
										T.EARLIEST_RPT_TO_CNTY_DT,T.EARLIEST_RPT_TO_STATE_DT ,T.DIAGNOSIS_DATE,
										T.ILLNESS_ONSET_DATE, T.CASE_STATUS ,T.MMWR_WEEK ,T.MMWR_YEAR ,T.INVESTIGATION_CREATE_DATE ,
										T.INVESTIGATION_CREATED_BY ,  T.INVESTIGATION_LAST_UPDTD_DATE ,T.NOTIFICATION_STATUS ,        
										T.INVESTIGATION_LAST_UPDTD_BY ,T.PROGRAM_JURISDICTION_OID ,T.PROGRAM_AREA,          
										T.PHYSICIAN_LAST_NAME	,T.PHYSICIAN_FIRST_NAME ,T.NOTIFICATION_LOCAL_ID ,      
										T.NOTIFICATION_CREATE_DATE,   	
									    T.NOTIFICATION_SUBMITTER ,  
										T.NOTIFICATION_LAST_UPDATED_DATE ,T.NOTIFICATION_LAST_UPDATED_USER,	
										T.FIRST_POSITIVE_CULTURE_DT ,T.INV_START_DT,T.HSPTL_ADMISSION_DT ,T.INV_RPT_DT, 	
										T.CONFIRMATION_DT,  T.CONFIRMATION_METHOD  ,T.CURR_PROCESS_STATE , T.JURISDICTION_NM ,     	
										T.PATIENT_COUNTY_CODE  ,T.LABORATORY_INFORMATION, T.EARLIEST_SPECIMEN_COLLECT_DATE ,
								        T.EVENT_DATE ,EVENT_DATE_TYPE
								 
								 
								  FROM dbo.TMP_INV_SUMM_DATAMART T
								 WHERE NOT EXISTS
								(
								  SELECT * 
									FROM dbo.INV_SUMM_DATAMART with (nolock)
								   WHERE INVESTIGATION_KEY = T.INVESTIGATION_KEY
								)order by INVESTIGATION_KEY
									
                              SELECT @ROWCOUNT_NO = @@ROWCOUNT;

									INSERT INTO [DBO].[JOB_FLOW_LOG] 
									(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
									VALUES(@BATCH_ID,'INV_SUMM_DATAMART','INV_SUMM_DATAMART','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


COMMIT TRANSACTION
	

				IF OBJECT_ID('dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT', 'U') IS NOT NULL   
				 drop table dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT;---1a

				IF OBJECT_ID('dbo.TMP_S_CONFIRMATION_METHOD_BASE', 'U') IS NOT NULL   
				drop table dbo.TMP_S_CONFIRMATION_METHOD_BASE  ;----3

				IF OBJECT_ID('dbo.TMP_S_CONFIRMATION_METHOD_PIVOT', 'U') IS NOT NULL
				DROP TABLE dbo.TMP_S_CONFIRMATION_METHOD_PIVOT;---4

				IF OBJECT_ID('dbo.TMP_S_PATIENT_LOCATION_KEY', 'U') IS NOT NULL   
				drop table dbo.TMP_S_PATIENT_LOCATION_KEY  ;---5

				IF OBJECT_ID('dbo.TMP_S_PATIENTS_INFO', 'U') IS NOT NULL   
				drop table dbo.TMP_S_PATIENTS_INFO  ;----6

				IF OBJECT_ID('dbo.TMP_S_PHYSICIANS_INFO', 'U') IS NOT NULL   
				drop table dbo.TMP_S_PHYSICIANS_INFO  ;---7

				IF OBJECT_ID('dbo.TMP_S_PATIENTS_DETAIL', 'U') IS NOT NULL   
				drop table dbo.TMP_S_PATIENTS_DETAIL  ;-----8

				IF OBJECT_ID('dbo.TMP_S_INV_WITH_USER', 'U') IS NOT NULL   
				drop table dbo.TMP_S_INV_WITH_USER  ;------9

				IF OBJECT_ID('dbo.TMP_S_INV_SUMM_DATAMART_INITI', 'U') IS NOT NULL   
				drop table dbo.TMP_S_INV_SUMM_DATAMART_INITI  ;---10

				IF OBJECT_ID('dbo.TMP_InvLab', 'U') IS NOT NULL   
				drop table dbo.TMP_InvLab ;----11

				IF OBJECT_ID('dbo.TMP_Lab', 'U') IS NOT NULL   
				drop table dbo.TMP_Lab ;----12

				IF OBJECT_ID(' dbo.TMP_BothTable', 'U') IS NOT NULL   
				drop table  dbo.TMP_BothTable ;---13

				IF OBJECT_ID('dbo.TMP_Inv2Labs', 'U') IS NOT NULL   
				 drop table dbo.TMP_Inv2Labs ;----14

				IF OBJECT_ID('dbo.TMP_SPECIMEN_COLLECTION', 'U') IS NOT NULL   
				 drop table dbo.TMP_SPECIMEN_COLLECTION ;----15

				IF OBJECT_ID(' dbo.TMP_CASE_LAB_DATAMART_MODIFIED', 'U') IS NOT NULL   
				drop table  dbo.TMP_CASE_LAB_DATAMART_MODIFIED ;---16

				IF OBJECT_ID('dbo.TMP_INV_SUMM_DATAMART', 'U') IS NOT NULL   
				drop table dbo.TMP_INV_SUMM_DATAMART ;----17				
				IF OBJECT_ID('dbo.TMP_S_INV_SUMM_DATAMART_INIT', 'U') IS NOT NULL   
				drop table dbo.TMP_S_INV_SUMM_DATAMART_INIT ;----18

-----------------------------------------------------------------------------
        BEGIN TRANSACTION ;
	
				SET @Proc_Step_no = 20;
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
					   'INV_SUMM_DATAMART'
					   ,'INV_SUMM_DATAMART'
					   ,'COMPLETE'
					   ,@Proc_Step_no
					   ,@Proc_Step_name
					   ,@RowCount_no
					   );
			  
	
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
						VALUES( @Batch_id, 'INV_SUMM_DATAMART', 'INV_SUMM_DATAMART', 'ERROR', @Proc_Step_no, 'ERROR - '+@Proc_Step_name, 'Step -'+CAST(@Proc_Step_no AS varchar(3))+' -'+CAST(@ErrorMessage AS varchar(500)), 0 );
						RETURN -1;
	        END CATCH;
END;
GO
