USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[sp_LDF_VACCINE_PREVENT_DISEASES_DATAMART]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER OFF
GO

CREATE PROCEDURE [dbo].[sp_LDF_VACCINE_PREVENT_DISEASES_DATAMART]
  @batch_id BIGINT
 as

  BEGIN

    DECLARE @RowCount_no INT;
    DECLARE @Proc_Step_no FLOAT = 0;
    DECLARE @Proc_Step_Name VARCHAR(200) = '';
	DECLARE @batch_start_time datetime2(7) = null;
	DECLARE @batch_end_time datetime2(7) = null;
 
	DECLARE @cols  AS NVARCHAR(MAX)='';
	DECLARE @query AS NVARCHAR(MAX)='';

	DECLARE @Alterdynamiccolumnlist varchar(max)=''
	DECLARE @dynamiccolumnUpdate varchar(max)=''
	DECLARE @dynamiccolumninsert varchar(max)=''
	
	DECLARE @dynamiccolumnList varchar(max)=''	--insert into LDF_VACCINE_PREVENT_DISEASES table from TMP_VACCINE_PREVENT_DISEASES table
	DECLARE @count BIGINT;
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
           ,'LDF_VACCINE_PREVENT_DISEASES'
           ,'LDF_VACCINE_PREVENT_DISEASES'
		   ,'START'
		   ,@Proc_Step_no
		   ,@Proc_Step_Name
           ,0
		   );
  
    COMMIT TRANSACTION;
	
	
	SELECT @batch_start_time = batch_start_dttm,@batch_end_time = batch_end_dttm
	FROM [dbo].[job_batch_log]
	WHERE status_type = 'start';

		SET @count =
	(
		SELECT COUNT(1)
		FROM dbo.S_LDF_DIMENSIONAL_DATA with (nolock)
			 INNER JOIN dbo.LDF_DATAMART_TABLE_REF with (nolock) ON S_LDF_DIMENSIONAL_DATA.PHC_CD = dbo.LDF_DATAMART_TABLE_REF.condition_cd
														  AND DATAMART_NAME = 'LDF_VACCINE_PREVENT_DISEASES'
	);	
		
		IF (@count > 0)
		BEGIN
			
			BEGIN TRANSACTION;

				SET @PROC_STEP_NO = 2;
				SET @PROC_STEP_NAME = ' GENERATING TMP_BASE_VACCINE_PREVENT_DISEASES'; 

				IF OBJECT_ID('dbo.TMP_BASE_VACCINE_PREVENT_DISEASES', 'U') IS NOT NULL  
						DROP TABLE dbo.TMP_BASE_VACCINE_PREVENT_DISEASES;

				SELECT S_LDF_DIMENSIONAL_DATA.*
							INTO dbo.TMP_BASE_VACCINE_PREVENT_DISEASES
							FROM dbo.S_LDF_DIMENSIONAL_DATA with (nolock)
								 INNER JOIN dbo.LDF_DATAMART_TABLE_REF with (nolock) ON PHC_CD = LDF_DATAMART_TABLE_REF.CONDITION_CD
																			  AND DATAMART_NAME = 'LDF_VACCINE_PREVENT_DISEASES';
					
				SELECT @ROWCOUNT_NO = @@ROWCOUNT;

				INSERT INTO [DBO].[JOB_FLOW_LOG] 
					(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
					VALUES(@BATCH_ID,'LDF_VACCINE_PREVENT_DISEASES','LDF_VACCINE_PREVENT_DISEASES','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

				COMMIT TRANSACTION;
			
				BEGIN TRANSACTION;

				SET @PROC_STEP_NO = 3;
				SET @PROC_STEP_NAME = ' GENERATING TMP_LINKED_VAC_PREVENT_MEASLES'; 
				
			
				IF OBJECT_ID('dbo.TMP_LINKED_VAC_PREVENT_MEASLES', 'U') IS NOT NULL  
						DROP TABLE dbo.TMP_LINKED_VAC_PREVENT_MEASLES;


						SELECT GEN_LDF.*, 
							INV.INVESTIGATION_KEY, 
							INV.INV_LOCAL_ID 'INVESTIGATION_LOCAL_ID', 
							INV.CASE_OID 'PROGRAM_JURISDICTION_OID',
							GEN.PATIENT_KEY,
							PATIENT.PATIENT_LOCAL_ID 'PATIENT_LOCAL_ID',
							CONDITION.CONDITION_SHORT_NM 'DISEASE_NAME'
						INTO  dbo.TMP_LINKED_VAC_PREVENT_MEASLES
						FROM
							dbo.TMP_BASE_VACCINE_PREVENT_DISEASES GEN_LDF with (nolock)
							INNER JOIN  dbo.INVESTIGATION INV with (nolock)
						ON  
							GEN_LDF.INVESTIGATION_UID=INV.CASE_UID 
						INNER JOIN dbo.MEASLES_CASE GEN with (nolock)
						ON 
							GEN.INVESTIGATION_KEY=INV.INVESTIGATION_KEY
						INNER JOIN dbo.CONDITION with (nolock)
						ON 
							CONDITION.CONDITION_KEY= GEN.CONDITION_KEY
						INNER JOIN dbo.D_PATIENT PATIENT with (nolock) 
						ON 
							PATIENT.PATIENT_KEY=GEN.PATIENT_KEY
						ORDER BY 
							INVESTIGATION_UID;
						
								 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

				 INSERT INTO [DBO].[JOB_FLOW_LOG] 
					(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
					VALUES(@BATCH_ID,'LDF_VACCINE_PREVENT_DISEASES','LDF_VACCINE_PREVENT_DISEASES','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

				COMMIT TRANSACTION;
			
				BEGIN TRANSACTION;

				SET @PROC_STEP_NO = 4;
				SET @PROC_STEP_NAME = ' GENERATING TMP_LINKED_VAC_PREVENT_PERTUSSIS'; 
				
			
				IF OBJECT_ID('dbo.TMP_LINKED_VAC_PREVENT_PERTUSSIS', 'U') IS NOT NULL  
						DROP TABLE dbo.TMP_LINKED_VAC_PREVENT_PERTUSSIS;


						SELECT GEN_LDF.*, 
							INV.INVESTIGATION_KEY, 
							INV.INV_LOCAL_ID 'INVESTIGATION_LOCAL_ID', 
							INV.CASE_OID 'PROGRAM_JURISDICTION_OID',
							GEN.PATIENT_KEY,
							PATIENT.PATIENT_LOCAL_ID 'PATIENT_LOCAL_ID',
							CONDITION.CONDITION_SHORT_NM 'DISEASE_NAME'
						INTO  dbo.TMP_LINKED_VAC_PREVENT_PERTUSSIS
						FROM
							dbo.TMP_BASE_VACCINE_PREVENT_DISEASES GEN_LDF with (nolock)
							INNER JOIN  dbo.INVESTIGATION INV
						ON  
							GEN_LDF.INVESTIGATION_UID=INV.CASE_UID 
						INNER JOIN dbo.PERTUSSIS_CASE GEN with (nolock)
						ON 
							GEN.INVESTIGATION_KEY=INV.INVESTIGATION_KEY
						INNER JOIN dbo.CONDITION with (nolock)
						ON 
							CONDITION.CONDITION_KEY= GEN.CONDITION_KEY
						INNER JOIN dbo.D_PATIENT PATIENT with (nolock)
						ON 
							PATIENT.PATIENT_KEY=GEN.PATIENT_KEY
						ORDER BY 
							INVESTIGATION_UID;
						
								 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

				 INSERT INTO [DBO].[JOB_FLOW_LOG] 
					(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
					VALUES(@BATCH_ID,'LDF_VACCINE_PREVENT_DISEASES','LDF_VACCINE_PREVENT_DISEASES','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

				COMMIT TRANSACTION;
				
				BEGIN TRANSACTION;

				SET @PROC_STEP_NO = 5;
				SET @PROC_STEP_NAME = ' GENERATING TMP_LINKED_VAC_PREVENT_RUBELLA'; 
				
			
				IF OBJECT_ID('dbo.TMP_LINKED_VAC_PREVENT_RUBELLA', 'U') IS NOT NULL  
						DROP TABLE dbo.TMP_LINKED_VAC_PREVENT_RUBELLA;


						SELECT GEN_LDF.*, 
							INV.INVESTIGATION_KEY, 
							INV.INV_LOCAL_ID 'INVESTIGATION_LOCAL_ID', 
							INV.CASE_OID 'PROGRAM_JURISDICTION_OID',
							GEN.PATIENT_KEY,
							PATIENT.PATIENT_LOCAL_ID 'PATIENT_LOCAL_ID',
							CONDITION.CONDITION_SHORT_NM 'DISEASE_NAME'
						INTO  dbo.TMP_LINKED_VAC_PREVENT_RUBELLA
						FROM
							dbo.TMP_BASE_VACCINE_PREVENT_DISEASES GEN_LDF with (nolock)
							INNER JOIN  dbo.INVESTIGATION INV with (nolock)
						ON  
							GEN_LDF.INVESTIGATION_UID=INV.CASE_UID 
						INNER JOIN dbo.RUBELLA_CASE GEN with (nolock)
						ON 
							GEN.INVESTIGATION_KEY=INV.INVESTIGATION_KEY
						INNER JOIN dbo.CONDITION with (nolock)
						ON 
							CONDITION.CONDITION_KEY= GEN.CONDITION_KEY
						INNER JOIN dbo.D_PATIENT PATIENT with (nolock)
						ON 
							PATIENT.PATIENT_KEY=GEN.PATIENT_KEY
						ORDER BY 
							INVESTIGATION_UID;
						
								 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

				 INSERT INTO [DBO].[JOB_FLOW_LOG] 
					(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
					VALUES(@BATCH_ID,'LDF_VACCINE_PREVENT_DISEASES','LDF_VACCINE_PREVENT_DISEASES','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

				COMMIT TRANSACTION;
				
				BEGIN TRANSACTION;

				SET @PROC_STEP_NO = 6;
				SET @PROC_STEP_NAME = ' GENERATING TMP_LINKED_VAC_PREVENT_CRS'; 
				
			
				IF OBJECT_ID('dbo.TMP_LINKED_VAC_PREVENT_CRS', 'U') IS NOT NULL  
						DROP TABLE dbo.TMP_LINKED_VAC_PREVENT_CRS;


						SELECT GEN_LDF.*, 
							INV.INVESTIGATION_KEY, 
							INV.INV_LOCAL_ID 'INVESTIGATION_LOCAL_ID', 
							INV.CASE_OID 'PROGRAM_JURISDICTION_OID',
							GEN.PATIENT_KEY,
							PATIENT.PATIENT_LOCAL_ID 'PATIENT_LOCAL_ID',
							CONDITION.CONDITION_SHORT_NM 'DISEASE_NAME'
						INTO  dbo.TMP_LINKED_VAC_PREVENT_CRS
						FROM
							dbo.TMP_BASE_VACCINE_PREVENT_DISEASES GEN_LDF with (nolock)
							INNER JOIN  dbo.INVESTIGATION INV with (nolock)
						ON  
							GEN_LDF.INVESTIGATION_UID=INV.CASE_UID 
						INNER JOIN dbo.CRS_CASE GEN with (nolock)
						ON 
							GEN.INVESTIGATION_KEY=INV.INVESTIGATION_KEY
						INNER JOIN dbo.CONDITION with (nolock)
						ON 
							CONDITION.CONDITION_KEY= GEN.CONDITION_KEY
						INNER JOIN dbo.D_PATIENT PATIENT  with (nolock)
						ON 
							PATIENT.PATIENT_KEY=GEN.PATIENT_KEY
						ORDER BY 
							INVESTIGATION_UID;
						
								 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

				 INSERT INTO [DBO].[JOB_FLOW_LOG] 
					(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
					VALUES(@BATCH_ID,'LDF_VACCINE_PREVENT_DISEASES','LDF_VACCINE_PREVENT_DISEASES','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

				COMMIT TRANSACTION;
				
				BEGIN TRANSACTION;

				--DATA ALL_VAC_PREVENT_MERGED ;
				--MERGE LINKED_VAC_PREVENT_MEASLES LINKED_VAC_PREVENT_CRS LINKED_VAC_PREVENT_PERTUSSIS LINKED_VAC_PREVENT_RUBELLA;
				--BY PATIENT_KEY;

				SET @PROC_STEP_NO = 7;
				SET @PROC_STEP_NAME = ' GENERATING TMP_ALL_VAC_PREVENT_MERGED'; 

					IF OBJECT_ID('dbo.TMP_ALL_VAC_PREVENT_MERGED', 'U') IS NOT NULL  
						 DROP TABLE dbo.TMP_ALL_VAC_PREVENT_MERGED;
						
						-- INSERT DATA INTO TMP_ALL_VAC_PREVENT_MERGED from TMP_LINKED_VAC_PREVENT_MEASLES, TMP_LINKED_VAC_PREVENT_CRS, TMP_LINKED_VAC_PREVENT_PERTUSSIS and TMP_LINKED_VAC_PREVENT_RUBELLA
						
						SELECT *
						INTO dbo.TMP_ALL_VAC_PREVENT_MERGED
						FROM dbo.TMP_LINKED_VAC_PREVENT_MEASLES with (nolock);

						INSERT INTO dbo.TMP_ALL_VAC_PREVENT_MERGED
						SELECT *
						FROM dbo.TMP_LINKED_VAC_PREVENT_CRS with (nolock);

						INSERT INTO dbo.TMP_ALL_VAC_PREVENT_MERGED
						SELECT *
						FROM dbo.TMP_LINKED_VAC_PREVENT_PERTUSSIS with (nolock);

						INSERT INTO dbo.TMP_ALL_VAC_PREVENT_MERGED
						SELECT *
						FROM dbo.TMP_LINKED_VAC_PREVENT_RUBELLA with (nolock);
						
						SELECT @ROWCOUNT_NO = @@ROWCOUNT;
						
						DELETE FROM dbo.TMP_ALL_VAC_PREVENT_MERGED WHERE INVESTIGATION_KEY IS NULL;
						  
				 INSERT INTO [DBO].[JOB_FLOW_LOG] 
					(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
					VALUES(@BATCH_ID,'LDF_VACCINE_PREVENT_DISEASES','LDF_VACCINE_PREVENT_DISEASES','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

				COMMIT TRANSACTION;
				
				BEGIN TRANSACTION;

				SET @PROC_STEP_NO = 8;
				SET @PROC_STEP_NAME = ' GENERATING TMP_ALL_VAC_PREVENT_DIS'; 

			-- CREATE TABLE ALL_VACCINE_PREVENT_DISEASES

				IF OBJECT_ID('dbo.TMP_ALL_VAC_PREVENT_DIS', 'U') IS NOT NULL  
						 DROP TABLE dbo.TMP_ALL_VAC_PREVENT_DIS;

						SELECT A.*, 
						B.DATAMART_COLUMN_NM 'DM',
						A.phc_cd 'DISEASE_CD',
						A.page_set 'DISEASE_NM'
						INTO dbo.TMP_ALL_VAC_PREVENT_DIS
						FROM dbo.LDF_DATAMART_COLUMN_REF  B with (nolock) 
						FULL OUTER JOIN dbo.TMP_ALL_VAC_PREVENT_MERGED A with (nolock)
						ON A.LDF_UID= B.LDF_UID WHERE
						(B.LDF_PAGE_SET ='VPD'
						OR B.CONDITION_CD IN (SELECT CONDITION_CD FROM 
							dbo.LDF_DATAMART_TABLE_REF with (nolock) WHERE DATAMART_NAME = 'LDF_VACCINE_PREVENT_DISEASES') 
						)
						ORDER BY INVESTIGATION_UID;

					
						UPDATE dbo.TMP_ALL_VAC_PREVENT_DIS
						SET DISEASE_CD = CONDITION_CD
						WHERE DATALENGTH(REPLACE(CONDITION_CD, ' ', ''))>1;  
					
						UPDATE dbo.TMP_ALL_VAC_PREVENT_DIS
						SET DISEASE_CD = PHC_CD
						WHERE DATALENGTH(REPLACE(CONDITION_CD, ' ', ''))<=1;  

						UPDATE dbo.TMP_ALL_VAC_PREVENT_DIS
						SET DISEASE_NM= PAGE_SET
						WHERE DATALENGTH(DISEASE_NM)<2;

						UPDATE dbo.TMP_ALL_VAC_PREVENT_DIS
						SET DATAMART_COLUMN_NM=DM
						WHERE DATALENGTH(DM)>2;  


					SELECT @ROWCOUNT_NO = @@ROWCOUNT;

				 INSERT INTO [DBO].[JOB_FLOW_LOG] 
					(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
					VALUES(@BATCH_ID,'LDF_VACCINE_PREVENT_DISEASES','LDF_VACCINE_PREVENT_DISEASES','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

				COMMIT TRANSACTION;
			
				BEGIN TRANSACTION;

				SET @PROC_STEP_NO = 9;
				SET @PROC_STEP_NAME = ' GENERATING TMP_ALL_VAC_PREVENT_DIS_SHORT_COL'; 
					
				IF OBJECT_ID('dbo.TMP_ALL_VAC_PREVENT_DIS_SHORT_COL', 'U') IS NOT NULL  
						 DROP TABLE dbo.TMP_ALL_VAC_PREVENT_DIS_SHORT_COL;

					
						SELECT INVESTIGATION_KEY,
								INVESTIGATION_LOCAL_ID,
								PROGRAM_JURISDICTION_OID,
								PATIENT_KEY,
								PATIENT_LOCAL_ID,
								DISEASE_NAME,
								DISEASE_CD,
								DATAMART_COLUMN_NM,
								col1 
						INTO dbo.TMP_ALL_VAC_PREVENT_DIS_SHORT_COL
						FROM dbo.TMP_ALL_VAC_PREVENT_DIS with (nolock)
						WHERE data_type IN ('CV', 'ST');

					
					SELECT @ROWCOUNT_NO = @@ROWCOUNT;

				 INSERT INTO [DBO].[JOB_FLOW_LOG] 
					(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
					VALUES(@BATCH_ID,'LDF_VACCINE_PREVENT_DISEASES','LDF_VACCINE_PREVENT_DISEASES','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

				COMMIT TRANSACTION;
			
				
				BEGIN TRANSACTION;

				SET @PROC_STEP_NO = 10;
				SET @PROC_STEP_NAME = ' GENERATING TMP_ALL_VAC_PREVENT_DIS_TA'; 

					IF OBJECT_ID('dbo.TMP_ALL_VAC_PREVENT_DIS_TA', 'U') IS NOT NULL  
						DROP TABLE dbo.TMP_ALL_VAC_PREVENT_DIS_TA;

					
						SELECT INVESTIGATION_KEY,
								INVESTIGATION_LOCAL_ID,
								PROGRAM_JURISDICTION_OID,
								PATIENT_KEY,
								PATIENT_LOCAL_ID,
								DISEASE_NAME,
								DISEASE_CD,
								DATAMART_COLUMN_NM,
								col1 
						INTO dbo.TMP_ALL_VAC_PREVENT_DIS_TA
						FROM dbo.TMP_ALL_VAC_PREVENT_DIS with (nolock) 
						WHERE data_type IN ('LIST_ST');   
					
			
				SELECT @ROWCOUNT_NO = @@ROWCOUNT;

				INSERT INTO [DBO].[JOB_FLOW_LOG] 
					(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
					VALUES(@BATCH_ID,'LDF_VACCINE_PREVENT_DISEASES','LDF_VACCINE_PREVENT_DISEASES','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

				COMMIT TRANSACTION;
			
				---- create table ldf_base_clinical_translated
				
				BEGIN TRANSACTION;

				SET @PROC_STEP_NO = 11;
				SET @PROC_STEP_NAME = ' GENERATING TMP_VAC_PREVENT_DIS_TA'; 
				set @count = (SELECT count(*) FROM TMP_ALL_VAC_PREVENT_DIS_TA)
				IF @count>0
				BEGIN
					IF OBJECT_ID('dbo.TMP_VAC_PREVENT_DIS_TA', 'U') IS NOT NULL  
							DROP TABLE dbo.TMP_VAC_PREVENT_DIS_TA;

						
							ALTER TABLE TMP_ALL_VAC_PREVENT_DIS_TA
							ADD ANSWERCOL varchar(8000);

							UPDATE TMP_ALL_VAC_PREVENT_DIS_TA
							SET ANSWERCOL = SUBSTRING(COL1, 1, 8000); 

							ALTER TABLE TMP_ALL_VAC_PREVENT_DIS_TA
							DROP COLUMN COL1;

							--DECLARE @cols  AS NVARCHAR(MAX)='';
							--DECLARE @query AS NVARCHAR(MAX)='';
							SET @cols='';
							SET @query='';

							SELECT @cols = @cols + QUOTENAME(DATAMART_COLUMN_NM) + ',' FROM (select distinct DATAMART_COLUMN_NM from TMP_ALL_VAC_PREVENT_DIS_TA ) as tmp
							select @cols = substring(@cols, 0, len(@cols)) --trim "," at end

							--PRINT CAST(@cols AS NVARCHAR(3000))
							set @query = 
							'SELECT *
							INTO TMP_VAC_PREVENT_DIS_TA
							fROM
							( 
							SELECT     INVESTIGATION_KEY,
									   INVESTIGATION_LOCAL_ID,
									   PROGRAM_JURISDICTION_OID,
									   PATIENT_KEY,
									   PATIENT_LOCAL_ID,
									   DISEASE_NAME,
									   DISEASE_CD,
									   DATAMART_COLUMN_NM,
									   ANSWERCOL
								
							FROM dbo.TMP_ALL_VAC_PREVENT_DIS_TA with (nolock) )
							as A 

							PIVOT ( MAX([ANSWERCOL]) FOR DATAMART_COLUMN_NM   IN (' + @cols + ')) AS PivotTable';
							execute(@query)
				
								 SELECT @ROWCOUNT_NO = @@ROWCOUNT;
				END	
				 INSERT INTO [DBO].[JOB_FLOW_LOG] 
					(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
					VALUES(@BATCH_ID,'LDF_VACCINE_PREVENT_DISEASES','LDF_VACCINE_PREVENT_DISEASES','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

				COMMIT TRANSACTION;
				
				-- If data does not exist create TMP_VAC_PREVENT_DIS_TA table same as TMP_ALL_VAC_PREVENT_DIS_TA, which will be used while merging table in step 9
				set @count = (SELECT count(*) FROM dbo.TMP_ALL_VAC_PREVENT_DIS_TA)
				IF @count=0
				BEGIN
					IF OBJECT_ID('dbo.TMP_VAC_PREVENT_DIS_TA', 'U') IS NOT NULL  
						DROP TABLE dbo.TMP_VAC_PREVENT_DIS_TA;
							
						SELECT INVESTIGATION_KEY,
							   INVESTIGATION_LOCAL_ID,
							   PROGRAM_JURISDICTION_OID,
							   PATIENT_KEY,
							   PATIENT_LOCAL_ID,
							   DISEASE_NAME,
							   DISEASE_CD
						INTO dbo.TMP_VAC_PREVENT_DIS_TA
						FROM dbo.TMP_ALL_VAC_PREVENT_DIS_TA with (nolock);
						
				END	
				
				BEGIN TRANSACTION;

				SET @PROC_STEP_NO = 12;
				SET @PROC_STEP_NAME = ' GENERATING TMP_VAC_PREVENT_DIS_SHORT_COL'; 
				set @count = (SELECT count(*) FROM dbo.TMP_ALL_VAC_PREVENT_DIS_SHORT_COL)
				IF @count>0
					BEGIN
					
						IF OBJECT_ID('dbo.TMP_VAC_PREVENT_DIS_SHORT_COL', 'U') IS NOT NULL  
							 DROP TABLE dbo.TMP_VAC_PREVENT_DIS_SHORT_COL;

				
							ALTER TABLE dbo.TMP_ALL_VAC_PREVENT_DIS_SHORT_COL
							ADD ANSWERCOL varchar(8000);

							UPDATE dbo.TMP_ALL_VAC_PREVENT_DIS_SHORT_COL
							SET ANSWERCOL = SUBSTRING(COL1, 1, 8000); 

							ALTER TABLE dbo.TMP_ALL_VAC_PREVENT_DIS_SHORT_COL
							DROP COLUMN COL1;

							--DECLARE @cols  AS NVARCHAR(MAX)='';
							--DECLARE @query AS NVARCHAR(MAX)='';
							SET @cols='';
							SET @query='';
							BEGIN TRY
								SELECT @cols = @cols + QUOTENAME(DATAMART_COLUMN_NM) + ',' FROM (select distinct DATAMART_COLUMN_NM from dbo.TMP_ALL_VAC_PREVENT_DIS_SHORT_COL ) as tmp
								select @cols = substring(@cols, 0, len(@cols)) --trim "," at end

								set @query = 
								'SELECT *
								INTO TMP_VAC_PREVENT_DIS_SHORT_COL
								FROM
								( 
								SELECT     INVESTIGATION_KEY,
										   INVESTIGATION_LOCAL_ID,
										   PROGRAM_JURISDICTION_OID,
										   PATIENT_KEY,
										   PATIENT_LOCAL_ID,
										   DISEASE_NAME,
										   DISEASE_CD,
										   DATAMART_COLUMN_NM,
										   ANSWERCOL
									
								FROM dbo.TMP_ALL_VAC_PREVENT_DIS_SHORT_COL with (nolock) )
								as A 

								PIVOT ( MAX([ANSWERCOL]) FOR DATAMART_COLUMN_NM   IN (' + @cols + ')) AS PivotTable';
								execute(@query)
							END TRY  
							BEGIN CATCH
									DECLARE @ErrorNumber1 INT = ERROR_NUMBER();
									DECLARE @ErrorLine1 INT = ERROR_LINE();
									DECLARE @ErrorMessage1 NVARCHAR(4000) = ERROR_MESSAGE();

								IF @ErrorNumber1=511
								BEGIN
									SET @cols='';
									SET @query='';
									SELECT @cols = @cols + QUOTENAME(DATAMART_COLUMN_NM) + ',' FROM (select distinct top 300 DATAMART_COLUMN_NM from dbo.TMP_ALL_VAC_PREVENT_DIS_SHORT_COL ) as tmp
									select @cols = substring(@cols, 0, len(@cols)) --trim "," at end

									IF OBJECT_ID('dbo.TMP_VAC_PREVENT_DIS_SHORT_COL', 'U') IS NOT NULL  
									DROP TABLE dbo.TMP_VAC_PREVENT_DIS_SHORT_COL;

									set @query = 
									'SELECT *
									INTO TMP_VAC_PREVENT_DIS_SHORT_COL
									FROM
									( 
									SELECT     INVESTIGATION_KEY,
											   INVESTIGATION_LOCAL_ID,
											   PROGRAM_JURISDICTION_OID,
											   PATIENT_KEY,
											   PATIENT_LOCAL_ID,
											   DISEASE_NAME,
											   DISEASE_CD,
											   DATAMART_COLUMN_NM,
											   ANSWERCOL
										
									FROM dbo.TMP_ALL_VAC_PREVENT_DIS_SHORT_COL with (nolock) )
									as A 

									PIVOT ( MAX([ANSWERCOL]) FOR DATAMART_COLUMN_NM   IN (' + @cols + ')) AS PivotTable';
									execute(@query)
								END
								ELSE
									THROW @ErrorNumber1, @ErrorMessage1, @ErrorMessage1;
							END CATCH
						END	
							 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

				 INSERT INTO [DBO].[JOB_FLOW_LOG] 
					(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
					VALUES(@BATCH_ID,'LDF_VACCINE_PREVENT_DISEASES','LDF_VACCINE_PREVENT_DISEASES','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

				COMMIT TRANSACTION;
				
				-- If data does not exist create TMP_VAC_PREVENT_DIS_SHORT_COL table same as TMP_ALL_VAC_PREVENT_DIS_SHORT_COL, which will be used while merging table in step 9
				set @count = (SELECT count(*) FROM dbo.TMP_ALL_VAC_PREVENT_DIS_SHORT_COL)
				IF @count=0
				BEGIN
				IF OBJECT_ID('dbo.TMP_VAC_PREVENT_DIS_SHORT_COL', 'U') IS NOT NULL  
						DROP TABLE dbo.TMP_VAC_PREVENT_DIS_SHORT_COL;
					
					SELECT INVESTIGATION_KEY,
						   INVESTIGATION_LOCAL_ID,
						   PROGRAM_JURISDICTION_OID,
						   PATIENT_KEY,
						   PATIENT_LOCAL_ID,
						   DISEASE_NAME,
						   DISEASE_CD
						INTO dbo.TMP_VAC_PREVENT_DIS_SHORT_COL
						FROM dbo.TMP_ALL_VAC_PREVENT_DIS_SHORT_COL with (nolock);
				END	
			
				BEGIN TRANSACTION;

				SET @PROC_STEP_NO = 13;
				SET @PROC_STEP_NAME = ' GENERATING TMP_VACCINE_PREVENT_DISEASES'; 

					IF OBJECT_ID('dbo.TMP_VACCINE_PREVENT_DISEASES', 'U') IS NOT NULL  
						 DROP TABLE dbo.TMP_VACCINE_PREVENT_DISEASES;

						EXECUTE  [dbo].[sp_MERGE_TWO_TABLES] 
						   @INPUT_TABLE1='dbo.TMP_VAC_PREVENT_DIS_SHORT_COL'
						  ,@INPUT_TABLE2='dbo.TMP_VAC_PREVENT_DIS_TA'
						  ,@OUTPUT_TABLE='dbo.TMP_VACCINE_PREVENT_DISEASES'
						  ,@JOIN_ON_COLUMN='INVESTIGATION_KEY';

						SELECT @ROWCOUNT_NO = @@ROWCOUNT;
					
						DELETE FROM dbo.TMP_VACCINE_PREVENT_DISEASES WHERE INVESTIGATION_KEY IS NULL;


				 INSERT INTO [DBO].[JOB_FLOW_LOG] 
					(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
					VALUES(@BATCH_ID,'LDF_VACCINE_PREVENT_DISEASES','LDF_VACCINE_PREVENT_DISEASES','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

				COMMIT TRANSACTION;
			
				--- data  ldf_base_country_translated; length x $4000; length col1 $4000; 
				--- line 308 to 316


				SET @PROC_STEP_NO = 14;
				SET @PROC_STEP_NAME = ' GENERATING LDF_VACCINE_PREVENT_DISEASES'; 

					IF OBJECT_ID('dbo.LDF_VACCINE_PREVENT_DISEASES', 'U') IS NOT NULL
					BEGIN
						BEGIN TRANSACTION;
							SET @Alterdynamiccolumnlist='';
							SET @dynamiccolumnUpdate='';
							 
							SELECT   @Alterdynamiccolumnlist  = @Alterdynamiccolumnlist+ 'ALTER TABLE dbo.LDF_VACCINE_PREVENT_DISEASES ADD [' + name   +  '] varchar(4000) ',
								@dynamiccolumnUpdate= @dynamiccolumnUpdate + 'LDF_VACCINE_PREVENT_DISEASES.[' +  name  + ']='  + 'dbo.TMP_VACCINE_PREVENT_DISEASES.['  +name  + '] ,'
							FROM  Sys.Columns WHERE Object_ID = Object_ID('dbo.TMP_VACCINE_PREVENT_DISEASES')
							AND name NOT IN  ( SELECT name FROM  Sys.Columns WHERE Object_ID = Object_ID('LDF_VACCINE_PREVENT_DISEASES'))
							
							
							--PRINT '@@Alterdynamiccolumnlist -----------	'+CAST(@Alterdynamiccolumnlist AS NVARCHAR(max))
							--PRINT '@@@@dynamiccolumnUpdate -----------	'+CAST(@dynamiccolumnUpdate AS NVARCHAR(max))

							IF @Alterdynamiccolumnlist IS NOT NULL AND @Alterdynamiccolumnlist!=''
							BEGIN

								EXEC(  @Alterdynamiccolumnlist)

								SET  @dynamiccolumnUpdate=SUBSTRING(@dynamiccolumnUpdate,1,LEN(@dynamiccolumnUpdate)-1)

								EXEC ('update  dbo.LDF_VACCINE_PREVENT_DISEASES  SET ' +   @dynamiccolumnUpdate + ' FROM dbo.TMP_VACCINE_PREVENT_DISEASES     
								   inner join  dbo.LDF_VACCINE_PREVENT_DISEASES  on  dbo.TMP_VACCINE_PREVENT_DISEASES.INVESTIGATION_LOCAL_ID =  dbo.LDF_VACCINE_PREVENT_DISEASES.INVESTIGATION_LOCAL_ID')

							END
					
						COMMIT TRANSACTION;
					
						BEGIN TRANSACTION;
							--In case of updates, delete the existing ones and insert updated ones in LDF_VACCINE_PREVENT_DISEASES
							DELETE FROM dbo.LDF_VACCINE_PREVENT_DISEASES WHERE INVESTIGATION_KEY IN (SELECT INVESTIGATION_KEY FROM dbo.TMP_VACCINE_PREVENT_DISEASES);

						COMMIT TRANSACTION;
					
						BEGIN TRANSACTION;
					
						--- During update if TMP_VACCINE_PREVENT_DISEASES has 4 columns updated only and the LDF_VACCINE_PREVENT_DISEASES has 7 columns then get column name dynamically from TMP_VACCINE_PREVENT_DISEASES and populate them.
					
							SET @dynamiccolumnList =''
							SELECT @dynamiccolumnList= @dynamiccolumnList +'['+ name +'],' FROM  Sys.Columns WHERE Object_ID = Object_ID('dbo.TMP_VACCINE_PREVENT_DISEASES')
							SET  @dynamiccolumnList=SUBSTRING(@dynamiccolumnList,1,LEN(@dynamiccolumnList)-1)

							--PRINT '@@@@@dynamiccolumnList -----------	'+CAST(@dynamiccolumnList AS NVARCHAR(max))

							EXEC ('INSERT INTO dbo.LDF_VACCINE_PREVENT_DISEASES ('+@dynamiccolumnList+')
							SELECT '+@dynamiccolumnList +'
							FROM dbo.TMP_VACCINE_PREVENT_DISEASES');
					
							SELECT @ROWCOUNT_NO = @@ROWCOUNT;
							
						COMMIT TRANSACTION;
					END
					
					---- This is one time deal, if table does not exist then create it.
					IF OBJECT_ID('dbo.LDF_VACCINE_PREVENT_DISEASES', 'U') IS NULL 
					BEGIN
						BEGIN TRANSACTION;
						
							SELECT *
							INTO dbo.LDF_VACCINE_PREVENT_DISEASES
							FROM dbo.TMP_VACCINE_PREVENT_DISEASES;
						
							SELECT @ROWCOUNT_NO = @@ROWCOUNT;
							
						COMMIT TRANSACTION;
					END
					
				 INSERT INTO [DBO].[JOB_FLOW_LOG] 
					(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
					VALUES(@BATCH_ID,'LDF_VACCINE_PREVENT_DISEASES','LDF_VACCINE_PREVENT_DISEASES','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			
				
				--- DELETE Temp tables.
				
				IF OBJECT_ID('dbo.TMP_BASE_VACCINE_PREVENT_DISEASES', 'U') IS NOT NULL  
					 DROP TABLE dbo.TMP_BASE_VACCINE_PREVENT_DISEASES; 

				IF OBJECT_ID('dbo.TMP_ALL_VACCINE_PREVENT_DISEASES', 'U') IS NOT NULL  
						 DROP TABLE dbo.TMP_ALL_VACCINE_PREVENT_DISEASES; 

				IF OBJECT_ID('dbo.TMP_ALL_VAC_PREVENT_DIS_SHORT_COL', 'U') IS NOT NULL  
						 DROP TABLE dbo.TMP_ALL_VAC_PREVENT_DIS_SHORT_COL; 

				IF OBJECT_ID('dbo.TMP_ALL_VAC_PREVENT_DIS_TA', 'U') IS NOT NULL  
						 DROP TABLE dbo.TMP_ALL_VAC_PREVENT_DIS_TA; 

				IF OBJECT_ID('dbo.TMP_VAC_PREVENT_DIS_TA', 'U') IS NOT NULL  
						 DROP TABLE dbo.TMP_VAC_PREVENT_DIS_TA; 

				IF OBJECT_ID('dbo.TMP_VAC_PREVENT_DIS_SHORT_COL', 'U') IS NOT NULL  
						 DROP TABLE dbo.TMP_VAC_PREVENT_DIS_SHORT_COL; 
						 
				IF OBJECT_ID('dbo.TMP_VACCINE_PREVENT_DISEASES', 'U') IS NOT NULL  
						 DROP TABLE dbo.TMP_VACCINE_PREVENT_DISEASES; 
				
				IF OBJECT_ID('dbo.TMP_LINKED_VAC_PREVENT_MEASLES', 'U') IS NOT NULL  
					 DROP TABLE dbo.TMP_LINKED_VAC_PREVENT_MEASLES; 

				IF OBJECT_ID('dbo.TMP_LINKED_VAC_PREVENT_CRS', 'U') IS NOT NULL  
						 DROP TABLE dbo.TMP_LINKED_VAC_PREVENT_CRS; 

				IF OBJECT_ID('dbo.TMP_LINKED_VAC_PREVENT_PERTUSSIS', 'U') IS NOT NULL  
						 DROP TABLE dbo.TMP_LINKED_VAC_PREVENT_PERTUSSIS; 

				IF OBJECT_ID('dbo.TMP_LINKED_VAC_PREVENT_RUBELLA', 'U') IS NOT NULL  
						 DROP TABLE dbo.TMP_LINKED_VAC_PREVENT_RUBELLA; 
						 
				IF OBJECT_ID('dbo.TMP_ALL_VAC_PREVENT_MERGED', 'U') IS NOT NULL  
						 DROP TABLE dbo.TMP_ALL_VAC_PREVENT_MERGED; 

			END	-- END for IF EXISTS (SELECT * INTO dbo.TMP_BASE_VACCINE_PREVENT_DISEASES WHERE  PHC_CD IN .................  at line 69

    BEGIN TRANSACTION ;
	
	SET @Proc_Step_no = 15;
	SET @Proc_Step_Name = 'SP_COMPLETE'; 


	INSERT INTO [dbo].[job_flow_log] (
		    [batch_id]
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
           'LDF_VACCINE_PREVENT_DISEASES'
           ,'LDF_VACCINE_PREVENT_DISEASES'
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
           ,'LDF_VACCINE_PREVENT_DISEASES'
           ,'LDF_VACCINE_PREVENT_DISEASES'
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
